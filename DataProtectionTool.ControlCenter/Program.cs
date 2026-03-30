using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Azure.Data.Tables;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Parquet;
using DataProtectionTool.Contracts;
using DataProtectionTool.ControlCenter.Interceptors;
using DataProtectionTool.ControlCenter.Models;
using DataProtectionTool.ControlCenter.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8190, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1;
    });
    options.ListenAnyIP(8191, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<SecretValidationInterceptor>();
});

builder.Services.AddSingleton<AgentRegistry>();

var tableConnectionString = builder.Configuration.GetSection("AzureTableStorage")["ConnectionString"]
    ?? throw new InvalidOperationException("AzureTableStorage:ConnectionString is not configured.");
var tableServiceClient = new TableServiceClient(tableConnectionString);
builder.Services.AddSingleton(tableServiceClient);
builder.Services.AddSingleton(sp => new ClientTableService(
    sp.GetRequiredService<TableServiceClient>(),
    "Users",
    "ControlCenter",
    "DataItem",
    sp.GetRequiredService<ILogger<ClientTableService>>()));

var blobSection = builder.Configuration.GetSection("AzureBlobStorage");
var blobStorageConfig = new BlobStorageConfig
{
    StorageAccount = blobSection["StorageAccount"] ?? "",
    Container = blobSection["Container"] ?? "",
    AccessKey = blobSection["AccessKey"] ?? ""
};
builder.Services.AddSingleton(blobStorageConfig);

var blobCredential = new StorageSharedKeyCredential(blobStorageConfig.StorageAccount, blobStorageConfig.AccessKey);
var blobServiceUri = blobStorageConfig.StorageAccount == "devstoreaccount1"
    ? new Uri($"http://127.0.0.1:10000/{blobStorageConfig.StorageAccount}")
    : new Uri($"https://{blobStorageConfig.StorageAccount}.blob.core.windows.net");
var blobServiceClient = new BlobServiceClient(blobServiceUri, blobCredential);
builder.Services.AddSingleton(blobServiceClient);
builder.Services.AddSingleton(blobCredential);

var dataEngineConfig = builder.Configuration.GetSection("DataEngine").Get<DataEngineConfig>()
    ?? throw new InvalidOperationException("DataEngine section is not configured in appsettings.");
builder.Services.AddSingleton(dataEngineConfig);

var app = builder.Build();
var previewFilenameRegex = new Regex(
    "^preview_(\\d+)_([0-9a-fA-F]{32})(?:_([2-9]\\d*))?\\.parquet$",
    RegexOptions.Compiled | RegexOptions.CultureInvariant);

{
    var containerClient = app.Services.GetRequiredService<BlobServiceClient>()
        .GetBlobContainerClient(blobStorageConfig.Container);
    await containerClient.CreateIfNotExistsAsync();
}

{
    var usersTable = tableServiceClient.GetTableClient("Users");
    await usersTable.CreateIfNotExistsAsync();
    var controlCenterTable = tableServiceClient.GetTableClient("ControlCenter");
    await controlCenterTable.CreateIfNotExistsAsync();
    var dataItemTable = tableServiceClient.GetTableClient("DataItem");
    await dataItemTable.CreateIfNotExistsAsync();
}

app.UseStaticFiles();

app.MapGrpcService<AgentHubService>();
app.MapGet("/", () => "DataProtectionTool ControlCenter is running.");

app.MapGet("/api/agents/{path}", (string path, AgentRegistry registry) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    return Results.Ok(new
    {
        oid = info.Oid,
        tid = info.Tid,
        agentId = info.AgentId,
        connectedAt = info.ConnectedAt.ToString("O")
    });
});

app.MapGet("/api/agents/{path}/user-id", async (string path, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
    var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
    return Results.Ok(new { uniqueId });
});

app.MapPost("/api/agents/{path}/validate-sql", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("validate_sql", body, TimeSpan.FromSeconds(30));

        using var doc = JsonDocument.Parse(result);
        var message = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;
        var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

        _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql",
            success ? "SQL validation: success" : $"SQL validation: failed — {message}",
            message ?? "");

        return Results.Ok(new { success, message = message ?? "Unknown result" });
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", "SQL validation: timeout", "Agent did not respond within 30 seconds.");
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", $"SQL validation: error — {ex.Message}");
        return Results.Ok(new { success = false, message = $"Validation error: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/save-connection", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
        var entity = await clientTableService.SaveConnectionAsync(
            partitionKey,
            root.TryGetProperty("serverName", out var sn) ? sn.GetString() ?? "" : "",
            root.TryGetProperty("authentication", out var au) ? au.GetString() ?? "" : "",
            root.TryGetProperty("userName", out var un) ? un.GetString() ?? "" : "",
            root.TryGetProperty("password", out var pw) ? pw.GetString() ?? "" : "",
            root.TryGetProperty("databaseName", out var db) ? db.GetString() ?? "" : "",
            root.TryGetProperty("encrypt", out var en) ? en.GetString() ?? "" : "",
            root.TryGetProperty("trustServerCertificate", out var tsc) && tsc.GetBoolean());

        if (registry.TryGetConnection(path, out var connection) && connection is not null)
        {
            try
            {
                var connections = await clientTableService.GetConnectionsAsync(partitionKey);
                var connectionsJson = JsonSerializer.Serialize(connections.Select(c => new
                {
                    rowKey = c.RowKey,
                    connectionType = c.ConnectionType,
                    serverName = c.ServerName,
                    authentication = c.Authentication,
                    userName = c.UserName,
                    password = c.Password,
                    databaseName = c.DatabaseName,
                    encrypt = c.Encrypt,
                    trustServerCertificate = c.TrustServerCertificate,
                }));
                await connection.ResponseStream.WriteAsync(new ServerMessage
                {
                    Type = "connections_list",
                    Payload = connectionsJson
                });
            }
            catch
            {
            }
        }

        var serverName = root.TryGetProperty("serverName", out var snEvt) ? snEvt.GetString() ?? "" : "";
        _ = clientTableService.AppendEventAsync(partitionKey, "save_connection", $"Connection saved: {serverName}");

        return Results.Ok(new
        {
            success = true,
            rowKey = entity.RowKey,
            message = "Connection saved."
        });
    }
    catch (Exception ex)
    {
        var pk = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
        _ = clientTableService.AppendEventAsync(pk, "save_connection", $"Save connection failed: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Failed to save: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/list-tables", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    string rowKey;
    bool refresh = false;
    try
    {
        using var bodyDoc = JsonDocument.Parse(body);
        rowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
        refresh = bodyDoc.RootElement.TryGetProperty("refresh", out var rfEl) && rfEl.GetBoolean();
    }
    catch
    {
        return Results.BadRequest(new { error = "Invalid request body." });
    }

    var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
    if (connEntity == null)
        return Results.NotFound(new { error = "Connection not found." });

    if (!refresh)
    {
        var cached = await clientTableService.GetDataItemsAsync(partitionKey, connEntity.ServerName, connEntity.DatabaseName);
        if (cached.Count > 0)
        {
            var cachedTables = cached.Select(d => new { schema = d.Schema, name = d.TableName }).ToList();
            _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", $"Listed {cachedTables.Count} tables (cached)");
            return Results.Ok(new { success = true, tables = cachedTables });
        }
    }

    try
    {
        var result = await connection.SendCommandAsync("list_tables", body, TimeSpan.FromSeconds(30));

        try
        {
            using var doc = JsonDocument.Parse(result);
            if (doc.RootElement.TryGetProperty("tables", out var tEl) && tEl.ValueKind == JsonValueKind.Array)
            {
                var tableList = new List<(string schema, string name)>();
                foreach (var item in tEl.EnumerateArray())
                {
                    var schema = item.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
                    var name = item.TryGetProperty("name", out var nEl) ? nEl.GetString() ?? "" : "";
                    tableList.Add((schema, name));
                }
                var tables = tableList.Select(t => new { schema = t.schema, name = t.name }).ToList();

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await clientTableService.SaveDataItemsAsync(partitionKey, connEntity.ServerName, connEntity.DatabaseName, rowKey, tableList);
                        await clientTableService.AppendEventAsync(partitionKey, "list_tables", $"Listed {tableList.Count} tables");
                    }
                    catch (Exception saveEx)
                    {
                        await clientTableService.AppendEventAsync(partitionKey, "list_tables",
                            $"Failed to load tables from {connEntity.DatabaseName}. Refresh the database to try again.");
                        await clientTableService.AppendEventAsync(partitionKey, "list_tables",
                            saveEx.Message, saveEx.ToString());
                    }
                });

                return Results.Ok(new { success = true, tables });
            }
        }
        catch (Exception ex)
        {
            app.Logger.LogWarning(ex, "Failed to parse/save tables from agent response");
        }

        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", "List tables: timeout");
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", $"List tables error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"List tables error: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/preview-table", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    string connRowKey = "", schema = "", tName = "";
    try
    {
        using var bodyDoc = JsonDocument.Parse(body);
        connRowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
        schema = bodyDoc.RootElement.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
        tName = bodyDoc.RootElement.TryGetProperty("tableName", out var tEl) ? tEl.GetString() ?? "" : "";
    }
    catch { }

    var tableLabel = $"{schema}.{tName}";

    var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, connRowKey);
    DataItemEntity? dataItem = null;
    if (connEntity != null)
    {
        dataItem = await clientTableService.GetDataItemByTableAsync(
            partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tName);

        if (dataItem != null && !string.IsNullOrEmpty(dataItem.PreviewFileList))
        {
            var cachedFilenames = dataItem.PreviewFileList.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
            _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Preview table (cached): {tableLabel}");
            return Results.Ok(new { success = true, filenames = cachedFilenames, cached = true });
        }
    }

    try
    {
        var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
        if (string.IsNullOrWhiteSpace(uniqueId) || !IsDigitsOnly(uniqueId))
            return Results.BadRequest(new { success = false, message = "User unique ID is missing." });

        var requestBody = AddUniqueIdToPayload(body, uniqueId);
        var result = await connection.SendCommandAsync("preview_table", requestBody, TimeSpan.FromSeconds(60));

        if (dataItem != null)
        {
            try
            {
                using var resultDoc = JsonDocument.Parse(result);
                if (resultDoc.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean())
                {
                    var filenames = new List<string>();
                    if (resultDoc.RootElement.TryGetProperty("filenames", out var fnEl) && fnEl.ValueKind == JsonValueKind.Array)
                        filenames = fnEl.EnumerateArray().Select(e => e.GetString() ?? "").Where(f => f != "").ToList();
                    else if (resultDoc.RootElement.TryGetProperty("filename", out var fEl))
                        filenames = new List<string> { fEl.GetString() ?? "" };

                    if (filenames.Count > 0)
                    {
                        _ = clientTableService.UpdatePreviewFileListAsync(dataItem, string.Join(",", filenames));
                    }
                }
            }
            catch { }
        }

        _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Preview table: {tableLabel}");
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Preview table timeout: {tableLabel}");
        return Results.Ok(new { success = false, message = "Agent did not respond within 60 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Preview table error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Preview table error: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/reload-preview-table", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService, BlobServiceClient blobClient, BlobStorageConfig blobConfig) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    string connRowKey = "", schema = "", tName = "";
    try
    {
        using var bodyDoc = JsonDocument.Parse(body);
        connRowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
        schema = bodyDoc.RootElement.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
        tName = bodyDoc.RootElement.TryGetProperty("tableName", out var tEl) ? tEl.GetString() ?? "" : "";
    }
    catch
    {
        return Results.BadRequest(new { error = "Invalid request body." });
    }

    var tableLabel = $"{schema}.{tName}";
    var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, connRowKey);
    if (connEntity == null)
        return Results.NotFound(new { error = "Connection not found." });

    var dataItem = await clientTableService.GetDataItemByTableAsync(
        partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tName);

    if (dataItem != null && !string.IsNullOrEmpty(dataItem.PreviewFileList))
    {
        var oldFilenames = dataItem.PreviewFileList.Split(',', StringSplitOptions.RemoveEmptyEntries);
        var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
        foreach (var filename in oldFilenames)
        {
            try { await containerClient.GetBlobClient(filename).DeleteIfExistsAsync(); } catch { }
        }

        await clientTableService.UpdatePreviewFileListAsync(dataItem, "");
    }

    try
    {
        var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
        if (string.IsNullOrWhiteSpace(uniqueId) || !IsDigitsOnly(uniqueId))
            return Results.BadRequest(new { success = false, message = "User unique ID is missing." });

        var requestBody = AddUniqueIdToPayload(body, uniqueId);
        var result = await connection.SendCommandAsync("preview_table", requestBody, TimeSpan.FromSeconds(60));

        if (dataItem != null)
        {
            try
            {
                using var resultDoc = JsonDocument.Parse(result);
                if (resultDoc.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean())
                {
                    var filenames = new List<string>();
                    if (resultDoc.RootElement.TryGetProperty("filenames", out var fnEl) && fnEl.ValueKind == JsonValueKind.Array)
                        filenames = fnEl.EnumerateArray().Select(e => e.GetString() ?? "").Where(f => f != "").ToList();
                    else if (resultDoc.RootElement.TryGetProperty("filename", out var fEl))
                        filenames = new List<string> { fEl.GetString() ?? "" };

                    if (filenames.Count > 0)
                    {
                        _ = clientTableService.UpdatePreviewFileListAsync(dataItem, string.Join(",", filenames));
                    }
                }
            }
            catch { }
        }

        _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Reload preview table: {tableLabel}");
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Reload preview table timeout: {tableLabel}");
        return Results.Ok(new { success = false, message = "Agent did not respond within 60 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_table", $"Reload preview table error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Reload preview table error: {ex.Message}" });
    }
});

app.MapGet("/api/blob/{filename}", async (string filename, BlobServiceClient blobClient, BlobStorageConfig blobConfig) =>
{
    if (!IsValidPreviewFilename(filename))
        return Results.BadRequest(new { error = "Invalid filename." });

    try
    {
        var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
        var blob = containerClient.GetBlobClient(filename);

        if (!await blob.ExistsAsync())
            return Results.NotFound(new { error = "Blob not found." });

        var download = await blob.DownloadContentAsync();
        using var ms = new MemoryStream(download.Value.Content.ToArray());
        using var reader = await ParquetReader.CreateAsync(ms);

        var dataFields = reader.Schema.GetDataFields();
        var headers = dataFields.Select(f => f.Name).ToList();
        var rows = new List<List<string?>>();

        for (int g = 0; g < reader.RowGroupCount; g++)
        {
            using var groupReader = reader.OpenRowGroupReader(g);
            var columns = new Array[dataFields.Length];
            int rowCount = 0;

            for (int c = 0; c < dataFields.Length; c++)
            {
                var col = await groupReader.ReadColumnAsync(dataFields[c]);
                columns[c] = col.Data;
                rowCount = col.Data.Length;
            }

            for (int r = 0; r < rowCount; r++)
            {
                var row = new List<string?>();
                for (int c = 0; c < dataFields.Length; c++)
                {
                    var val = columns[c].GetValue(r);
                    row.Add(val?.ToString() ?? "");
                }
                rows.Add(row);
            }
        }

        return Results.Json(new { headers, rows });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Failed to read blob: {ex.Message}");
    }
});

app.MapPost("/api/blob/preview-merge", async (HttpRequest request, BlobServiceClient blobClient, BlobStorageConfig blobConfig) =>
{
    string body;
    using (var sr = new StreamReader(request.Body))
        body = await sr.ReadToEndAsync();

    try
    {
        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("filenames", out var filenamesEl)
            || filenamesEl.ValueKind != JsonValueKind.Array
            || filenamesEl.GetArrayLength() == 0)
        {
            return Results.BadRequest(new { error = "filenames array is required." });
        }

        var filenames = filenamesEl.EnumerateArray().Select(e => e.GetString() ?? "").ToList();
        if (filenames.Any(f => !IsValidPreviewFilename(f)))
            return Results.BadRequest(new { error = "Invalid filename in list." });

        var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
        List<string>? headers = null;
        var rows = new List<List<string?>>();

        foreach (var filename in filenames)
        {
            var blob = containerClient.GetBlobClient(filename);
            if (!await blob.ExistsAsync())
                return Results.NotFound(new { error = $"Blob not found: {filename}" });

            var download = await blob.DownloadContentAsync();
            using var ms = new MemoryStream(download.Value.Content.ToArray());
            using var reader = await ParquetReader.CreateAsync(ms);

            var dataFields = reader.Schema.GetDataFields();
            var fileHeaders = dataFields.Select(f => f.Name).ToList();

            if (headers == null)
            {
                headers = fileHeaders;
            }
            else if (!headers.SequenceEqual(fileHeaders))
            {
                return Results.BadRequest(new { error = $"Schema mismatch in {filename}." });
            }

            for (int g = 0; g < reader.RowGroupCount; g++)
            {
                using var groupReader = reader.OpenRowGroupReader(g);
                var columns = new Array[dataFields.Length];
                int rowCount = 0;

                for (int c = 0; c < dataFields.Length; c++)
                {
                    var col = await groupReader.ReadColumnAsync(dataFields[c]);
                    columns[c] = col.Data;
                    rowCount = col.Data.Length;
                }

                for (int r = 0; r < rowCount; r++)
                {
                    var row = new List<string?>();
                    for (int c = 0; c < dataFields.Length; c++)
                    {
                        var val = columns[c].GetValue(r);
                        row.Add(val?.ToString() ?? "");
                    }
                    rows.Add(row);
                }
            }
        }

        return Results.Json(new { headers = headers ?? new List<string>(), rows });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Failed to merge blobs: {ex.Message}");
    }
});

app.MapPost("/api/blob/delete-preview", async (HttpRequest request, BlobServiceClient blobClient, BlobStorageConfig blobConfig) =>
{
    string body;
    using (var sr = new StreamReader(request.Body))
        body = await sr.ReadToEndAsync();

    try
    {
        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("filenames", out var filenamesEl)
            || filenamesEl.ValueKind != JsonValueKind.Array)
        {
            return Results.BadRequest(new { error = "filenames array is required." });
        }

        var filenames = filenamesEl.EnumerateArray().Select(e => e.GetString() ?? "").ToList();
        if (filenames.Any(f => !IsValidPreviewFilename(f)))
            return Results.BadRequest(new { error = "Invalid filename in list." });

        var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
        int deleted = 0;
        foreach (var filename in filenames)
        {
            var blob = containerClient.GetBlobClient(filename);
            if (await blob.DeleteIfExistsAsync())
                deleted++;
        }

        return Results.Ok(new { success = true, deleted });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Failed to delete blobs: {ex.Message}");
    }
});

app.MapGet("/api/agents/{path}/connections", async (string path, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
    var connections = await clientTableService.GetConnectionsAsync(partitionKey);

    var result = connections.Select(c => new
    {
        rowKey = c.RowKey,
        connectionType = c.ConnectionType,
        serverName = c.ServerName,
        authentication = c.Authentication,
        databaseName = c.DatabaseName,
        encrypt = c.Encrypt,
        trustServerCertificate = c.TrustServerCertificate,
        createdAt = c.CreatedAt.ToString("O")
    });

    return Results.Ok(result);
});

app.MapPost("/api/agents/{path}/validate-query", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("validate_query", body, TimeSpan.FromSeconds(30));

        using var doc = JsonDocument.Parse(result);
        var message = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;
        var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

        _ = clientTableService.AppendEventAsync(partitionKey, "validate_query",
            success ? "Query validation: success" : $"Query validation: failed — {message}",
            message ?? "");

        return Results.Ok(new { success, message = message ?? "Unknown result" });
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", "Query validation: timeout");
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", $"Query validation error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Query validation error: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/save-query", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        var connectionRowKey = root.TryGetProperty("connectionRowKey", out var crk) ? crk.GetString() ?? "" : "";
        var queryText = root.TryGetProperty("queryText", out var qt) ? qt.GetString() ?? "" : "";

        var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
        var entity = await clientTableService.SaveQueryAsync(partitionKey, connectionRowKey, queryText);

        _ = clientTableService.AppendEventAsync(partitionKey, "save_query", "Query saved");

        return Results.Ok(new
        {
            success = true,
            rowKey = entity.RowKey,
            message = "Query saved."
        });
    }
    catch (Exception ex)
    {
        var pk = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
        _ = clientTableService.AppendEventAsync(pk, "save_query", $"Save query failed: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Failed to save query: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/preview-query", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
        if (string.IsNullOrWhiteSpace(uniqueId) || !IsDigitsOnly(uniqueId))
            return Results.BadRequest(new { success = false, message = "User unique ID is missing." });

        var requestBody = AddUniqueIdToPayload(body, uniqueId);
        var result = await connection.SendCommandAsync("preview_query", requestBody, TimeSpan.FromSeconds(60));
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_query", "Preview query completed");
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_query", "Preview query: timeout");
        return Results.Ok(new { success = false, message = "Agent did not respond within 60 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "preview_query", $"Preview query error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Preview query error: {ex.Message}" });
    }
});

app.MapGet("/api/agents/{path}/queries", async (string path, string connectionRowKey, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
    var queries = await clientTableService.GetQueriesAsync(partitionKey, connectionRowKey);

    var result = queries.Select(q => new
    {
        rowKey = q.RowKey,
        connectionRowKey = q.ConnectionRowKey,
        queryText = q.QueryText,
        createdAt = q.CreatedAt.ToString("O")
    });

    return Results.Ok(result);
});

app.MapPost("/api/agents/{path}/dry-run", async (string path, HttpRequest request, AgentRegistry registry,
    ClientTableService clientTableService, DataEngineConfig dataEngineConfig) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        using var bodyDoc = JsonDocument.Parse(body);
        var root = bodyDoc.RootElement;
        var rowKey = root.GetProperty("rowKey").GetString() ?? "";
        var schema = root.GetProperty("schema").GetString() ?? "";
        var tableName = root.GetProperty("tableName").GetString() ?? "";

        var previewFilenames = new List<string>();
        if (root.TryGetProperty("previewBlobFilenames", out var fnamesEl) && fnamesEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in fnamesEl.EnumerateArray())
            {
                var fname = el.GetString();
                if (!string.IsNullOrEmpty(fname))
                    previewFilenames.Add(fname);
            }
        }

        if (previewFilenames.Count == 0)
            return Results.Ok(new { success = false, message = "No preview files available. Please preview the table first." });

        if (string.IsNullOrEmpty(dataEngineConfig.EngineUrl) || string.IsNullOrEmpty(dataEngineConfig.AuthorizationToken))
            return Results.Ok(new { success = false, message = "Data engine is not configured. Set EngineUrl and AuthorizationToken in appsettings.json." });

        if (string.IsNullOrEmpty(dataEngineConfig.ConnectorId))
            return Results.Ok(new { success = false, message = "Data engine ConnectorId is not configured. Set ConnectorId in appsettings.json." });

        if (string.IsNullOrEmpty(dataEngineConfig.ProfileSetId))
            return Results.Ok(new { success = false, message = "Data engine ProfileSetId is not configured. Set ProfileSetId in appsettings.json." });

        // Step 1: Get or create file format
        var connEntityForFormat = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
        DataItemEntity? dataItemForFormat = null;
        string fileFormatId = "";

        if (connEntityForFormat != null)
        {
            dataItemForFormat = await clientTableService.GetDataItemByTableAsync(
                partitionKey, connEntityForFormat.ServerName, connEntityForFormat.DatabaseName, schema, tableName);
            if (dataItemForFormat != null && !string.IsNullOrEmpty(dataItemForFormat.FileFormatId))
            {
                fileFormatId = dataItemForFormat.FileFormatId;
            }
        }

        if (string.IsNullOrEmpty(fileFormatId))
        {
            var commandPayload = JsonSerializer.Serialize(new
            {
                engineUrl = dataEngineConfig.EngineUrl,
                authToken = dataEngineConfig.AuthorizationToken,
                blobFilename = previewFilenames[0],
                fileFormatType = "PARQUET"
            });

            var agentResult = await connection.SendCommandAsync("create_file_format", commandPayload, TimeSpan.FromSeconds(120));

            using var resultDoc = JsonDocument.Parse(agentResult);
            var resultRoot = resultDoc.RootElement;

            if (!(resultRoot.TryGetProperty("success", out var successEl) && successEl.GetBoolean()))
            {
                var message = resultRoot.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : "File format creation failed.";
                return Results.Ok(new { success = false, message });
            }

            fileFormatId = resultRoot.TryGetProperty("fileFormatId", out var ffiEl)
                ? ffiEl.GetString() ?? "" : "";

            if (!string.IsNullOrEmpty(fileFormatId) && dataItemForFormat != null)
            {
                await clientTableService.UpdateFileFormatIdAsync(dataItemForFormat, fileFormatId);
            }
        }

        // Step 2: Create a file ruleset (new ruleset every dry run)
        var dryRunUuid = Guid.NewGuid().ToString("N");
        var rulesetName = $"ruleset_{dryRunUuid}";
        var rulesetPayload = JsonSerializer.Serialize(new
        {
            engineUrl = dataEngineConfig.EngineUrl,
            authToken = dataEngineConfig.AuthorizationToken,
            rulesetName,
            fileConnectorId = dataEngineConfig.ConnectorId
        });

        var rulesetResult = await connection.SendCommandAsync("create_file_ruleset", rulesetPayload, TimeSpan.FromSeconds(120));

        using var rulesetDoc = JsonDocument.Parse(rulesetResult);
        var rulesetRoot = rulesetDoc.RootElement;

        if (!(rulesetRoot.TryGetProperty("success", out var rulesetSuccessEl) && rulesetSuccessEl.GetBoolean()))
        {
            var rulesetMsg = rulesetRoot.TryGetProperty("message", out var rmEl) ? rmEl.GetString() : "File ruleset creation failed.";
            return Results.Ok(new { success = false, message = rulesetMsg });
        }

        var fileRulesetId = rulesetRoot.TryGetProperty("fileRulesetId", out var friEl)
            ? friEl.GetString() ?? "" : "";

        // Step 3: Create file metadata for each preview file
        var fileMetadataIds = new List<string>();
        foreach (var previewFile in previewFilenames)
        {
            var metadataPayload = JsonSerializer.Serialize(new
            {
                engineUrl = dataEngineConfig.EngineUrl,
                authToken = dataEngineConfig.AuthorizationToken,
                fileName = previewFile,
                rulesetId = fileRulesetId,
                fileFormatId,
                fileType = "PARQUET"
            });

            var metadataResult = await connection.SendCommandAsync("create_file_metadata", metadataPayload, TimeSpan.FromSeconds(120));

            using var metadataDoc = JsonDocument.Parse(metadataResult);
            var metadataRoot = metadataDoc.RootElement;

            if (!(metadataRoot.TryGetProperty("success", out var metaSuccessEl) && metaSuccessEl.GetBoolean()))
            {
                var metaMsg = metadataRoot.TryGetProperty("message", out var mmEl) ? mmEl.GetString() : $"File metadata creation failed for {previewFile}.";
                return Results.Ok(new { success = false, message = metaMsg });
            }

            var fileMetadataId = metadataRoot.TryGetProperty("fileMetadataId", out var fmiEl)
                ? fmiEl.GetString() ?? "" : "";
            fileMetadataIds.Add(fileMetadataId);
        }

        var engineBaseUrl = $"{dataEngineConfig.EngineUrl.TrimEnd('/')}/masking/api/v5.1.44";

        async Task<JsonDocument> RelayEngineHttpAsync(string method, string relativeUrl, object? requestBody = null)
        {
            var httpPayload = JsonSerializer.Serialize(new
            {
                method,
                url = $"{engineBaseUrl}/{relativeUrl}",
                headers = new Dictionary<string, string>
                {
                    ["accept"] = "application/json",
                    ["Authorization"] = dataEngineConfig.AuthorizationToken,
                    ["Content-Type"] = "application/json"
                },
                body = requestBody != null ? JsonSerializer.Serialize(requestBody) : null
            });
            var result = await connection.SendCommandAsync("http_request", httpPayload, TimeSpan.FromSeconds(120));
            return JsonDocument.Parse(result);
        }

        string ExtractBodyField(JsonDocument relayResponse, string fieldName)
        {
            if (!relayResponse.RootElement.TryGetProperty("body", out var bodyEl))
                return "";
            using var bodyDoc = JsonDocument.Parse(bodyEl.GetString() ?? "{}");
            return bodyDoc.RootElement.TryGetProperty(fieldName, out var valEl) ? valEl.ToString() : "";
        }

        // Step 4: Create profile job
        using var profileJobResp = await RelayEngineHttpAsync("POST", "profile-jobs", new
        {
            jobName = $"profile_{dryRunUuid}",
            profileSetId = int.Parse(dataEngineConfig.ProfileSetId),
            rulesetId = int.Parse(fileRulesetId)
        });

        if (!(profileJobResp.RootElement.TryGetProperty("success", out var pjSuccessEl) && pjSuccessEl.GetBoolean()))
        {
            var msg = profileJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job creation failed.";
            return Results.Ok(new { success = false, message = msg });
        }

        var profileJobId = ExtractBodyField(profileJobResp, "profileJobId");
        if (string.IsNullOrEmpty(profileJobId))
            return Results.Ok(new { success = false, message = "Profile job creation returned no profileJobId." });

        // Step 5: Create masking job
        using var maskingJobResp = await RelayEngineHttpAsync("POST", "masking-jobs", new
        {
            jobName = $"masking_{dryRunUuid}",
            rulesetId = int.Parse(fileRulesetId),
            onTheFlyMasking = false
        });

        if (!(maskingJobResp.RootElement.TryGetProperty("success", out var mjSuccessEl) && mjSuccessEl.GetBoolean()))
        {
            var msg = maskingJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job creation failed.";
            return Results.Ok(new { success = false, message = msg });
        }

        var maskingJobId = ExtractBodyField(maskingJobResp, "maskingJobId");
        if (string.IsNullOrEmpty(maskingJobId))
            return Results.Ok(new { success = false, message = "Masking job creation returned no maskingJobId." });

        // Step 6: Run the profile job
        using var profileExecResp = await RelayEngineHttpAsync("POST", "executions", new
        {
            jobId = int.Parse(profileJobId)
        });

        if (!(profileExecResp.RootElement.TryGetProperty("success", out var peSuccessEl) && peSuccessEl.GetBoolean()))
        {
            var msg = profileExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job execution failed to start.";
            return Results.Ok(new { success = false, message = msg });
        }

        var profileExecId = ExtractBodyField(profileExecResp, "executionId");
        if (string.IsNullOrEmpty(profileExecId))
            return Results.Ok(new { success = false, message = "Profile job execution returned no executionId." });

        // Step 7: Poll profile job status every 2 seconds
        var profileStatus = "";
        for (var i = 0; i < 300; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await RelayEngineHttpAsync("GET", $"executions/{profileExecId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            profileStatus = ExtractBodyField(statusResp, "status");
            if (profileStatus is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        if (profileStatus is not ("SUCCEEDED" or "WARNING"))
        {
            return Results.Ok(new { success = false, message = $"Profile job did not succeed. Final status: {profileStatus}" });
        }

        // Step 8: Run the masking job
        using var maskingExecResp = await RelayEngineHttpAsync("POST", "executions", new
        {
            jobId = int.Parse(maskingJobId)
        });

        if (!(maskingExecResp.RootElement.TryGetProperty("success", out var meSuccessEl) && meSuccessEl.GetBoolean()))
        {
            var msg = maskingExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job execution failed to start.";
            return Results.Ok(new { success = false, message = msg });
        }

        var maskingExecId = ExtractBodyField(maskingExecResp, "executionId");
        if (string.IsNullOrEmpty(maskingExecId))
            return Results.Ok(new { success = false, message = "Masking job execution returned no executionId." });

        // Step 9: Poll masking job status every 2 seconds
        var maskingStatus = "";
        for (var i = 0; i < 300; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await RelayEngineHttpAsync("GET", $"executions/{maskingExecId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            maskingStatus = ExtractBodyField(statusResp, "status");
            if (maskingStatus is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        if (maskingStatus is not ("SUCCEEDED" or "WARNING"))
        {
            return Results.Ok(new { success = false, message = $"Masking job did not succeed. Final status: {maskingStatus}" });
        }

        _ = clientTableService.AppendEventAsync(partitionKey, "dry_run",
            $"Dry run completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
            $"profileJobId={profileJobId} ({profileStatus}), maskingJobId={maskingJobId} ({maskingStatus})");

        return Results.Ok(new
        {
            success = true,
            fileFormatId,
            fileRulesetId,
            fileMetadataIds,
            profileJobId,
            profileStatus,
            maskingJobId,
            maskingStatus
        });
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "dry_run", "Dry run: timeout", "Agent did not respond within 120 seconds.");
        return Results.Ok(new { success = false, message = "Agent did not respond within 120 seconds." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "dry_run", $"Dry run error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Dry run error: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/http-request", async (string path, HttpRequest request, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("http_request", body, TimeSpan.FromSeconds(120));
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 120 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"HTTP request relay error: {ex.Message}" });
    }
});

app.MapGet("/api/agents/{path}/events", async (string path, AgentRegistry registry, ClientTableService clientTableService) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound(new { error = "Agent not found." });

    var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
    var events = await clientTableService.GetEventsAsync(partitionKey);

    var result = events.Select(e => new
    {
        timestamp = e.Timestamp.ToString("O"),
        type = e.Type,
        summary = e.Summary,
        detail = e.Detail
    });

    return Results.Ok(result);
});

app.MapGet("/agents/{path}", (string path, AgentRegistry registry, IWebHostEnvironment env) =>
{
    if (!registry.TryGet(path, out _))
        return Results.NotFound("Agent not found.");

    if (string.IsNullOrEmpty(env.WebRootPath))
        return Results.NotFound("Frontend not built. No wwwroot directory found. Run 'npm run build' in frontend/.");

    var indexPath = Path.Combine(env.WebRootPath, "index.html");
    if (!File.Exists(indexPath))
        return Results.NotFound("Frontend not built. Run 'npm run build' in frontend/.");

    return Results.Content(File.ReadAllText(indexPath), "text/html");
});

bool IsDigitsOnly(string value) => value.All(char.IsDigit);

string AddUniqueIdToPayload(string body, string uniqueId)
{
    JsonNode? payloadNode;
    try
    {
        payloadNode = JsonNode.Parse(body);
    }
    catch (JsonException ex)
    {
        throw new InvalidOperationException($"Invalid preview request payload: {ex.Message}", ex);
    }

    if (payloadNode is not JsonObject payloadObject)
        throw new InvalidOperationException("Invalid preview request payload.");

    payloadObject["uniqueId"] = uniqueId;
    return payloadObject.ToJsonString();
}

bool IsValidPreviewFilename(string filename) => previewFilenameRegex.IsMatch(filename);

await app.RunAsync();
