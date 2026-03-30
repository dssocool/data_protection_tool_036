using System.Text.Json;
using Azure.Data.Tables;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Parquet;
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
builder.Services.AddSingleton(new TableServiceClient(tableConnectionString));
builder.Services.AddSingleton<ClientTableService>();

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

var dataEngineConfigRoot = JsonSerializer.Deserialize<Dictionary<string, DataEngineConfig>>(
    File.ReadAllText("dataEngineConfig.json"),
    new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
    ?? throw new InvalidOperationException("Failed to parse dataEngineConfig.json.");
var dataEngineConfig = dataEngineConfigRoot.GetValueOrDefault("DataEngine")
    ?? throw new InvalidOperationException("dataEngineConfig.json is missing the 'DataEngine' section.");
builder.Services.AddSingleton(dataEngineConfig);

var app = builder.Build();

{
    var containerClient = app.Services.GetRequiredService<BlobServiceClient>()
        .GetBlobContainerClient(blobStorageConfig.Container);
    await containerClient.CreateIfNotExistsAsync();
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

app.MapPost("/api/agents/{path}/validate-sql", async (string path, HttpRequest request, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("validate_sql", body, TimeSpan.FromSeconds(30));

        using var doc = JsonDocument.Parse(result);
        var message = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;
        var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

        return Results.Ok(new { success, message = message ?? "Unknown result" });
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
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

        return Results.Ok(new
        {
            success = true,
            rowKey = entity.RowKey,
            message = "Connection saved."
        });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"Failed to save: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/list-tables", async (string path, HttpRequest request, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("list_tables", body, TimeSpan.FromSeconds(30));
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"List tables error: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/preview-table", async (string path, HttpRequest request, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("preview_table", body, TimeSpan.FromSeconds(60));
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 60 seconds." });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"Preview table error: {ex.Message}" });
    }
});

app.MapGet("/api/blob/{filename}", async (string filename, BlobServiceClient blobClient, BlobStorageConfig blobConfig) =>
{
    if (!filename.EndsWith("_preview.parquet"))
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

app.MapPost("/api/agents/{path}/validate-query", async (string path, HttpRequest request, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("validate_query", body, TimeSpan.FromSeconds(30));

        using var doc = JsonDocument.Parse(result);
        var message = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;
        var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

        return Results.Ok(new { success, message = message ?? "Unknown result" });
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
    }
    catch (Exception ex)
    {
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

        return Results.Ok(new
        {
            success = true,
            rowKey = entity.RowKey,
            message = "Query saved."
        });
    }
    catch (Exception ex)
    {
        return Results.Ok(new { success = false, message = $"Failed to save query: {ex.Message}" });
    }
});

app.MapPost("/api/agents/{path}/preview-query", async (string path, HttpRequest request, AgentRegistry registry) =>
{
    if (!registry.TryGetConnection(path, out var connection) || connection is null)
        return Results.NotFound(new { error = "Agent not found or not connected." });

    string body;
    using (var reader = new StreamReader(request.Body))
        body = await reader.ReadToEndAsync();

    try
    {
        var result = await connection.SendCommandAsync("preview_query", body, TimeSpan.FromSeconds(60));
        return Results.Content(result, "application/json");
    }
    catch (TimeoutException)
    {
        return Results.Ok(new { success = false, message = "Agent did not respond within 60 seconds." });
    }
    catch (Exception ex)
    {
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

await app.RunAsync();
