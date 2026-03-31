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
    AccessKey = blobSection["AccessKey"] ?? "",
    PreviewContainer = blobSection["PreviewContainer"] ?? ""
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
    "^(?:dryrun_[0-9a-fA-F]{32}_)?preview_(\\d+)_([0-9a-fA-F]{32})(?:_([2-9]\\d*))?\\.parquet$",
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
            var cachedTables = cached.Select(d => new { schema = d.Schema, name = d.TableName, fileFormatId = d.FileFormatId }).ToList();
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

app.MapGet("/api/agents/{path}/list-schemas", async (string path, string rowKey, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    if (string.IsNullOrEmpty(rowKey))
        return Results.BadRequest(new { error = "rowKey query parameter is required." });

    try
    {
        var payload = JsonSerializer.Serialize(new { rowKey });
        var result = await connection.SendCommandAsync("list_schemas", payload, TimeSpan.FromSeconds(30));
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"List schemas error: {ex.Message}" });
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

app.MapPost("/api/agents/{path}/reload-preview-table", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService, BlobServiceClient blobClient) =>
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
        var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
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

app.MapGet("/api/blob/{filename}", async (string filename, BlobServiceClient blobClient) =>
{
    if (!IsValidPreviewFilename(filename))
        return Results.BadRequest(new { error = "Invalid filename." });

    try
    {
        var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
        var blob = containerClient.GetBlobClient(filename);

        if (!await blob.ExistsAsync())
            return Results.NotFound(new { error = "Blob not found." });

        var download = await blob.DownloadContentAsync();
        using var ms = new MemoryStream(download.Value.Content.ToArray());
        using var reader = await ParquetReader.CreateAsync(ms);

        var dataFields = reader.Schema.GetDataFields();
        var headers = dataFields.Select(f => f.Name).ToList();
        string[]? columnTypes = null;
        if (reader.CustomMetadata != null
            && reader.CustomMetadata.TryGetValue("sql_types", out var sqlTypesJson))
        {
            columnTypes = JsonSerializer.Deserialize<string[]>(sqlTypesJson);
        }
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

        return Results.Json(new { headers, rows, columnTypes });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Failed to read blob: {ex.Message}");
    }
});

app.MapPost("/api/blob/preview-merge", async (HttpRequest request, BlobServiceClient blobClient) =>
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

        var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
        List<string>? headers = null;
        string[]? columnTypes = null;
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
                if (reader.CustomMetadata != null
                    && reader.CustomMetadata.TryGetValue("sql_types", out var sqlTypesJson))
                {
                    columnTypes = JsonSerializer.Deserialize<string[]>(sqlTypesJson);
                }
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

        return Results.Json(new { headers = headers ?? new List<string>(), rows, columnTypes });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Failed to merge blobs: {ex.Message}");
    }
});

app.MapPost("/api/blob/delete-preview", async (HttpRequest request, BlobServiceClient blobClient) =>
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

        var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
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

app.MapPost("/api/agents/{path}/dry-run", async (string path, HttpContext httpContext, AgentRegistry registry,
    ClientTableService clientTableService, DataEngineConfig dataEngineConfig,
    BlobServiceClient blobClient, BlobStorageConfig blobConfig) =>
{
    var response = httpContext.Response;
    var request = httpContext.Request;

    async Task WriteSseEvent(string eventType, string data)
    {
        await response.WriteAsync($"event: {eventType}\ndata: {data}\n\n");
        await response.Body.FlushAsync();
    }

    async Task WriteSseError(string message)
    {
        var json = JsonSerializer.Serialize(new { success = false, message });
        await WriteSseEvent("error", json);
    }

    if (!registry.TryGetConnection(path, out var connection) || connection is null)
    {
        response.StatusCode = 404;
        await response.WriteAsJsonAsync(new { error = "Agent not found or not connected." });
        return;
    }

    var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    response.ContentType = "text/event-stream";
    response.Headers["Cache-Control"] = "no-cache";
    response.Headers["Connection"] = "keep-alive";

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

        var previewHeaders = new List<string>();
        if (root.TryGetProperty("previewHeaders", out var headersEl) && headersEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in headersEl.EnumerateArray())
            {
                var h = el.GetString();
                if (h != null) previewHeaders.Add(h);
            }
        }

        var previewColumnTypes = new List<string>();
        if (root.TryGetProperty("previewColumnTypes", out var colTypesEl) && colTypesEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in colTypesEl.EnumerateArray())
            {
                var ct = el.GetString();
                if (ct != null) previewColumnTypes.Add(ct);
            }
        }

        if (previewFilenames.Count == 0)
        {
            await WriteSseError("No preview files available. Please preview the table first.");
            return;
        }

        if (string.IsNullOrEmpty(dataEngineConfig.EngineUrl) || string.IsNullOrEmpty(dataEngineConfig.AuthorizationToken))
        {
            await WriteSseError("Data engine is not configured. Set EngineUrl and AuthorizationToken in appsettings.json.");
            return;
        }

        if (string.IsNullOrEmpty(dataEngineConfig.ConnectorId))
        {
            await WriteSseError("Data engine ConnectorId is not configured. Set ConnectorId in appsettings.json.");
            return;
        }

        if (string.IsNullOrEmpty(dataEngineConfig.ProfileSetId))
        {
            await WriteSseError("Data engine ProfileSetId is not configured. Set ProfileSetId in appsettings.json.");
            return;
        }

        // Step 0: Copy preview files from data_preview container to configured (engine) container
        await WriteSseEvent("status", "Copying preview files...");
        var previewContainerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
        var engineContainerClient = blobClient.GetBlobContainerClient(blobConfig.Container);

        foreach (var previewFile in previewFilenames)
        {
            var sourceBlob = previewContainerClient.GetBlobClient(previewFile);
            var destBlob = engineContainerClient.GetBlobClient(previewFile);
            using var stream = new MemoryStream();
            await sourceBlob.DownloadToAsync(stream);
            stream.Position = 0;
            await destBlob.UploadAsync(stream, overwrite: true);
        }

        // Step 1: Get or create file format
        await WriteSseEvent("status", "Creating file format...");
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
                await WriteSseError(message ?? "File format creation failed.");
                return;
            }

            fileFormatId = resultRoot.TryGetProperty("fileFormatId", out var ffiEl)
                ? ffiEl.GetString() ?? "" : "";

            if (!string.IsNullOrEmpty(fileFormatId) && dataItemForFormat != null)
            {
                await clientTableService.UpdateFileFormatIdAsync(dataItemForFormat, fileFormatId);
            }
        }

        // Step 2: Create a file ruleset (new ruleset every dry run)
        await WriteSseEvent("status", "Creating file ruleset...");
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
            await WriteSseError(rulesetMsg ?? "File ruleset creation failed.");
            return;
        }

        var fileRulesetId = rulesetRoot.TryGetProperty("fileRulesetId", out var friEl)
            ? friEl.GetString() ?? "" : "";

        // Step 3: Create file metadata for each preview file
        var fileMetadataIds = new List<string>();
        for (var fi = 0; fi < previewFilenames.Count; fi++)
        {
            var previewFile = previewFilenames[fi];
            await WriteSseEvent("status", $"Creating file metadata... ({fi + 1} of {previewFilenames.Count})");

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
                await WriteSseError(metaMsg ?? $"File metadata creation failed for {previewFile}.");
                return;
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
        await WriteSseEvent("status", "Creating profile job...");
        using var profileJobResp = await RelayEngineHttpAsync("POST", "profile-jobs", new
        {
            jobName = $"profile_{dryRunUuid}",
            profileSetId = int.Parse(dataEngineConfig.ProfileSetId),
            rulesetId = int.Parse(fileRulesetId)
        });

        if (!(profileJobResp.RootElement.TryGetProperty("success", out var pjSuccessEl) && pjSuccessEl.GetBoolean()))
        {
            var msg = profileJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job creation failed.";
            await WriteSseError(msg ?? "Profile job creation failed.");
            return;
        }

        var profileJobId = ExtractBodyField(profileJobResp, "profileJobId");
        if (string.IsNullOrEmpty(profileJobId))
        {
            await WriteSseError("Profile job creation returned no profileJobId.");
            return;
        }

        // Step 5: Create masking job
        await WriteSseEvent("status", "Creating masking job...");
        using var maskingJobResp = await RelayEngineHttpAsync("POST", "masking-jobs", new
        {
            jobName = $"masking_{dryRunUuid}",
            rulesetId = int.Parse(fileRulesetId),
            onTheFlyMasking = false
        });

        if (!(maskingJobResp.RootElement.TryGetProperty("success", out var mjSuccessEl) && mjSuccessEl.GetBoolean()))
        {
            var msg = maskingJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job creation failed.";
            await WriteSseError(msg ?? "Masking job creation failed.");
            return;
        }

        var maskingJobId = ExtractBodyField(maskingJobResp, "maskingJobId");
        if (string.IsNullOrEmpty(maskingJobId))
        {
            await WriteSseError("Masking job creation returned no maskingJobId.");
            return;
        }

        // Step 6: Run the profile job
        await WriteSseEvent("status", "Running profile job...");
        using var profileExecResp = await RelayEngineHttpAsync("POST", "executions", new
        {
            jobId = int.Parse(profileJobId)
        });

        if (!(profileExecResp.RootElement.TryGetProperty("success", out var peSuccessEl) && peSuccessEl.GetBoolean()))
        {
            var msg = profileExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job execution failed to start.";
            await WriteSseError(msg ?? "Profile job execution failed to start.");
            return;
        }

        var profileExecId = ExtractBodyField(profileExecResp, "executionId");
        if (string.IsNullOrEmpty(profileExecId))
        {
            await WriteSseError("Profile job execution returned no executionId.");
            return;
        }

        // Step 7: Poll profile job status every 2 seconds
        var profileStatus = "";
        for (var i = 0; i < 300; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await RelayEngineHttpAsync("GET", $"executions/{profileExecId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            profileStatus = ExtractBodyField(statusResp, "status");
            await WriteSseEvent("status", $"Polling profile job: {profileStatus}...");
            if (profileStatus is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        if (profileStatus is not ("SUCCEEDED" or "WARNING"))
        {
            await WriteSseError($"Profile job did not succeed. Final status: {profileStatus}");
            return;
        }

        // Step 7.5: Check column rules for type mismatches (numeric SQL types mapped to STRING algorithms)
        if (previewHeaders.Count > 0 && previewColumnTypes.Count == previewHeaders.Count)
        {
            await WriteSseEvent("status", "Checking column rules for type mismatches...");

            var numericSqlTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "int", "bigint", "smallint", "tinyint", "float", "real",
                "decimal", "numeric", "money", "smallmoney", "bit"
            };

            var sqlTypeByColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < previewHeaders.Count; i++)
                sqlTypeByColumn[previewHeaders[i]] = previewColumnTypes[i];

            var columnRulesPayload = JsonSerializer.Serialize(new
            {
                fileFormatId,
                engineUrl = dataEngineConfig.EngineUrl,
                authToken = dataEngineConfig.AuthorizationToken
            });

            try
            {
                var columnRulesResult = await connection.SendCommandAsync("get_column_rules", columnRulesPayload, TimeSpan.FromSeconds(120));
                using var crDoc = JsonDocument.Parse(columnRulesResult);
                var crSuccess = crDoc.RootElement.TryGetProperty("success", out var crSuccessEl) && crSuccessEl.GetBoolean();

                if (crSuccess
                    && crDoc.RootElement.TryGetProperty("responseList", out var crListEl) && crListEl.ValueKind == JsonValueKind.Array
                    && crDoc.RootElement.TryGetProperty("algorithms", out var crAlgsEl) && crAlgsEl.ValueKind == JsonValueKind.Array)
                {
                    var algMaskTypes = new Dictionary<string, string>();
                    foreach (var alg in crAlgsEl.EnumerateArray())
                    {
                        var aName = alg.TryGetProperty("algorithmName", out var anEl) ? anEl.GetString() ?? "" : "";
                        var aMaskType = alg.TryGetProperty("maskType", out var mtEl) ? mtEl.GetString() ?? "" : "";
                        if (!string.IsNullOrEmpty(aName))
                            algMaskTypes[aName] = aMaskType;
                    }

                    var fixedCount = 0;
                    foreach (var rule in crListEl.EnumerateArray())
                    {
                        var fieldName = rule.TryGetProperty("fieldName", out var fnEl) ? fnEl.GetString() ?? "" : "";
                        var algName = rule.TryGetProperty("algorithmName", out var anEl) ? anEl.GetString() ?? "" : "";
                        var metadataId = rule.TryGetProperty("fileFieldMetadataId", out var idEl) ? idEl.ToString() : "";
                        var isMasked = !rule.TryGetProperty("isMasked", out var imEl) || imEl.ValueKind != JsonValueKind.False;

                        if (string.IsNullOrEmpty(fieldName) || string.IsNullOrEmpty(algName)
                            || string.IsNullOrEmpty(metadataId) || !isMasked)
                            continue;

                        if (!sqlTypeByColumn.TryGetValue(fieldName, out var sqlType))
                            continue;

                        if (!numericSqlTypes.Contains(sqlType))
                            continue;

                        if (!algMaskTypes.TryGetValue(algName, out var maskType) || maskType != "STRING")
                            continue;

                        await WriteSseEvent("status", $"Fixing type mismatch: {fieldName} ({sqlType}) mapped to STRING algorithm...");
                        using var fixResp = await RelayEngineHttpAsync("PUT", $"file-field-metadata/{metadataId}", new
                        {
                            isMasked = false,
                            isProfilerWritable = false
                        });
                        fixedCount++;
                    }

                    if (fixedCount > 0)
                        await WriteSseEvent("status", $"Fixed {fixedCount} type mismatch(es).");
                    else
                        await WriteSseEvent("status", "No type mismatches found.");
                }
            }
            catch (Exception ex)
            {
                await WriteSseEvent("status", $"Warning: Could not check type mismatches: {ex.Message}");
            }
        }

        // Step 8: Run the masking job
        await WriteSseEvent("status", "Running masking job...");
        using var maskingExecResp = await RelayEngineHttpAsync("POST", "executions", new
        {
            jobId = int.Parse(maskingJobId)
        });

        if (!(maskingExecResp.RootElement.TryGetProperty("success", out var meSuccessEl) && meSuccessEl.GetBoolean()))
        {
            var msg = maskingExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job execution failed to start.";
            await WriteSseError(msg ?? "Masking job execution failed to start.");
            return;
        }

        var maskingExecId = ExtractBodyField(maskingExecResp, "executionId");
        if (string.IsNullOrEmpty(maskingExecId))
        {
            await WriteSseError("Masking job execution returned no executionId.");
            return;
        }

        // Step 9: Poll masking job status every 2 seconds
        var maskingStatus = "";
        for (var i = 0; i < 300; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await RelayEngineHttpAsync("GET", $"executions/{maskingExecId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            maskingStatus = ExtractBodyField(statusResp, "status");
            await WriteSseEvent("status", $"Polling masking job: {maskingStatus}...");
            if (maskingStatus is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        if (maskingStatus is not ("SUCCEEDED" or "WARNING"))
        {
            await WriteSseError($"Masking job did not succeed. Final status: {maskingStatus}");
            return;
        }

        // Step 10: Copy masked files from engine container back to preview container
        await WriteSseEvent("status", "Copying masked results...");
        var maskedFilenames = new List<string>();
        foreach (var previewFile in previewFilenames)
        {
            var maskedBlob = engineContainerClient.GetBlobClient(previewFile);
            var maskedName = $"dryrun_{dryRunUuid}_{previewFile}";
            var destBlob = previewContainerClient.GetBlobClient(maskedName);
            using var maskedStream = new MemoryStream();
            await maskedBlob.DownloadToAsync(maskedStream);
            maskedStream.Position = 0;
            await destBlob.UploadAsync(maskedStream, overwrite: true);
            maskedFilenames.Add(maskedName);
        }

        _ = clientTableService.AppendEventAsync(partitionKey, "dry_run",
            $"Dry run completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
            $"profileJobId={profileJobId} ({profileStatus}), maskingJobId={maskingJobId} ({maskingStatus})");

        var completeJson = JsonSerializer.Serialize(new
        {
            success = true,
            fileFormatId,
            fileRulesetId,
            fileMetadataIds,
            profileJobId,
            profileStatus,
            maskingJobId,
            maskingStatus,
            maskedFilenames
        });
        await WriteSseEvent("complete", completeJson);
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "dry_run", "Dry run: timeout", "Agent did not respond within 120 seconds.");
        await WriteSseError("Agent did not respond within 120 seconds.");
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "dry_run", $"Dry run error: {ex.Message}");
        await WriteSseError($"Dry run error: {ex.Message}");
    }
});

app.MapPost("/api/agents/{path}/full-run", async (string path, HttpRequest request, AgentRegistry registry,
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

        if (string.IsNullOrEmpty(dataEngineConfig.EngineUrl) || string.IsNullOrEmpty(dataEngineConfig.AuthorizationToken))
            return Results.Ok(new { success = false, message = "Data engine is not configured. Set EngineUrl and AuthorizationToken in appsettings.json." });

        if (string.IsNullOrEmpty(dataEngineConfig.ConnectorId))
            return Results.Ok(new { success = false, message = "Data engine ConnectorId is not configured. Set ConnectorId in appsettings.json." });

        var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
        if (connEntity == null)
            return Results.Ok(new { success = false, message = "Connection not found." });

        var dataItem = await clientTableService.GetDataItemByTableAsync(
            partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tableName);

        var fileFormatId = dataItem != null ? dataItem.FileFormatId : "";
        if (string.IsNullOrEmpty(fileFormatId))
            return Results.Ok(new { success = false, message = "File format not found. Please run Dry Run first." });

        // Step 1: Export full table via agent
        var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
        if (string.IsNullOrWhiteSpace(uniqueId) || !IsDigitsOnly(uniqueId))
            return Results.BadRequest(new { success = false, message = "User unique ID is missing." });

        var exportPayload = JsonSerializer.Serialize(new { rowKey, schema, tableName, uniqueId });
        var exportResult = await connection.SendCommandAsync("export_table", exportPayload, TimeSpan.FromSeconds(600));

        using var exportDoc = JsonDocument.Parse(exportResult);
        var exportRoot = exportDoc.RootElement;

        if (!(exportRoot.TryGetProperty("success", out var exportSuccessEl) && exportSuccessEl.GetBoolean()))
        {
            var msg = exportRoot.TryGetProperty("message", out var mEl) ? mEl.GetString() : "Export failed.";
            return Results.Ok(new { success = false, message = msg });
        }

        var exportFilenames = new List<string>();
        if (exportRoot.TryGetProperty("filenames", out var fnEl) && fnEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in fnEl.EnumerateArray())
            {
                var fname = el.GetString();
                if (!string.IsNullOrEmpty(fname))
                    exportFilenames.Add(fname);
            }
        }

        if (exportFilenames.Count == 0)
            return Results.Ok(new { success = false, message = "Export produced no files." });

        _ = clientTableService.AppendEventAsync(partitionKey, "full_run",
            $"Full run: exported {exportFilenames.Count} file(s) for {schema}.{tableName}");

        // Step 2: Create file ruleset
        var fullRunUuid = Guid.NewGuid().ToString("N");
        var rulesetPayload = JsonSerializer.Serialize(new
        {
            engineUrl = dataEngineConfig.EngineUrl,
            authToken = dataEngineConfig.AuthorizationToken,
            rulesetName = $"fullrun_ruleset_{fullRunUuid}",
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

        // Step 3: Create file metadata for each exported file
        var fileMetadataIds = new List<string>();
        foreach (var exportFile in exportFilenames)
        {
            var metadataPayload = JsonSerializer.Serialize(new
            {
                engineUrl = dataEngineConfig.EngineUrl,
                authToken = dataEngineConfig.AuthorizationToken,
                fileName = exportFile,
                rulesetId = fileRulesetId,
                fileFormatId,
                fileType = "PARQUET"
            });

            var metadataResult = await connection.SendCommandAsync("create_file_metadata", metadataPayload, TimeSpan.FromSeconds(120));

            using var metadataDoc = JsonDocument.Parse(metadataResult);
            var metadataRoot = metadataDoc.RootElement;

            if (!(metadataRoot.TryGetProperty("success", out var metaSuccessEl) && metaSuccessEl.GetBoolean()))
            {
                var metaMsg = metadataRoot.TryGetProperty("message", out var mmEl) ? mmEl.GetString() : $"File metadata creation failed for {exportFile}.";
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

        // Step 4: Create masking job (skip profiling)
        using var maskingJobResp = await RelayEngineHttpAsync("POST", "masking-jobs", new
        {
            jobName = $"fullrun_masking_{fullRunUuid}",
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

        // Step 5: Execute masking job
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

        // Step 6: Poll masking job status
        var maskingStatus = "";
        for (var i = 0; i < 600; i++)
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

        _ = clientTableService.AppendEventAsync(partitionKey, "full_run",
            $"Full run completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
            $"maskingJobId={maskingJobId} ({maskingStatus}), files={exportFilenames.Count}");

        return Results.Ok(new
        {
            success = true,
            fileFormatId,
            fileRulesetId,
            fileMetadataIds,
            maskingJobId,
            maskingStatus,
            exportFilenames
        });
    }
    catch (TimeoutException)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "full_run", "Full run: timeout", "Agent did not respond in time.");
        return Results.Ok(new { success = false, message = "Agent did not respond in time." });
    }
    catch (Exception ex)
    {
        _ = clientTableService.AppendEventAsync(partitionKey, "full_run", $"Full run error: {ex.Message}");
        return Results.Ok(new { success = false, message = $"Full run error: {ex.Message}" });
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

app.MapGet("/api/agents/{path}/column-rules", async (string path, string? fileFormatId, AgentRegistry registry, DataEngineConfig dataEngineConfig) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    if (string.IsNullOrWhiteSpace(fileFormatId))
        return Results.Ok(new { success = false, message = "fileFormatId is required." });

    var commandPayload = JsonSerializer.Serialize(new
    {
        fileFormatId,
        engineUrl = dataEngineConfig.EngineUrl,
        authToken = dataEngineConfig.AuthorizationToken
    });

    try
    {
        var result = await connection.SendCommandAsync("get_column_rules", commandPayload, TimeSpan.FromSeconds(120));
        using var doc = JsonDocument.Parse(result);

        var success = doc.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean();
        if (!success)
        {
            var msg = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() ?? "Unknown error" : "Unknown error";
            return Results.Ok(new { success = false, message = msg });
        }

        object[] ExtractArray(string propName)
        {
            if (doc.RootElement.TryGetProperty(propName, out var el) && el.ValueKind == JsonValueKind.Array)
                return el.EnumerateArray().Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())!).ToArray();
            return Array.Empty<object>();
        }

        return Results.Ok(new
        {
            success = true,
            responseList = ExtractArray("responseList"),
            algorithms = ExtractArray("algorithms"),
            domains = ExtractArray("domains"),
            frameworks = ExtractArray("frameworks")
        });
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 120 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"Column rules fetch error: {ex.Message}" });
    }
});

app.MapPut("/api/agents/{path}/column-rule/{fileFieldMetadataId}", async (string path, string fileFieldMetadataId, HttpRequest request, AgentRegistry registry, DataEngineConfig dataEngineConfig) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    if (string.IsNullOrWhiteSpace(fileFieldMetadataId))
        return Results.Ok(new { success = false, message = "fileFieldMetadataId is required." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    using var bodyDoc = JsonDocument.Parse(body);
    var algorithmName = bodyDoc.RootElement.TryGetProperty("algorithmName", out var algEl) ? algEl.GetString() ?? "" : "";
    var domainName = bodyDoc.RootElement.TryGetProperty("domainName", out var domEl) ? domEl.GetString() ?? "" : "";

    var engineBaseUrl = $"{dataEngineConfig.EngineUrl.TrimEnd('/')}/masking/api/v5.1.44";

    var httpPayload = JsonSerializer.Serialize(new
    {
        method = "PUT",
        url = $"{engineBaseUrl}/file-field-metadata/{fileFieldMetadataId}",
        headers = new Dictionary<string, string>
        {
            ["accept"] = "application/json",
            ["Authorization"] = dataEngineConfig.AuthorizationToken,
            ["Content-Type"] = "application/json"
        },
        body = JsonSerializer.Serialize(new
        {
            algorithmName,
            domainName,
            isProfilerWritable = false
        })
    });

    try
    {
        var result = await connection.SendCommandAsync("http_request", httpPayload, TimeSpan.FromSeconds(120));
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 120 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"Column rule save error: {ex.Message}" });
    }
});

app.MapGet("/api/agents/{path}/engine-metadata", async (string path, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    try
    {
        var result = await connection.SendCommandAsync("get_engine_metadata", "{}", TimeSpan.FromSeconds(30));
        using var doc = JsonDocument.Parse(result);

        var success = doc.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean();
        if (!success)
        {
            var msg = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() ?? "Unknown error" : "Unknown error";
            return Results.Ok(new { success = false, message = msg });
        }

        object[] ExtractArray(string propName)
        {
            if (doc.RootElement.TryGetProperty(propName, out var el) && el.ValueKind == JsonValueKind.Array)
                return el.EnumerateArray().Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())!).ToArray();
            return Array.Empty<object>();
        }

        return Results.Ok(new
        {
            success = true,
            algorithms = ExtractArray("algorithms"),
            domains = ExtractArray("domains"),
            frameworks = ExtractArray("frameworks")
        });
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"Engine metadata fetch error: {ex.Message}" });
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
