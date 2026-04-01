using Azure.Data.Tables;
using Azure.Storage;
using Azure.Storage.Blobs;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using DataProtectionTool.Contracts;
using DataProtectionTool.HttpServer.Endpoints;
using DataProtectionTool.HttpServer.Models;
using DataProtectionTool.HttpServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8190, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1;
    });
});

var healthStatus = new CenterHealthStatus();
builder.Services.AddSingleton(healthStatus);

var rpcServerAddress = builder.Configuration.GetSection("RpcServer")["Address"];
if (string.IsNullOrEmpty(rpcServerAddress))
{
    healthStatus.ConfigurationErrors.Add("RpcServer:Address is not configured.");
}
else
{
    var grpcChannel = GrpcChannel.ForAddress(rpcServerAddress);
    var controlPlaneClient = new ControlPlane.ControlPlaneClient(grpcChannel);
    builder.Services.AddSingleton(controlPlaneClient);
}
builder.Services.AddSingleton<RpcAgentProxy>();

var tableConnectionString = builder.Configuration.GetSection("AzureTableStorage")["ConnectionString"];
TableServiceClient? tableServiceClient = null;

if (string.IsNullOrEmpty(tableConnectionString))
{
    healthStatus.ConfigurationErrors.Add("AzureTableStorage:ConnectionString is not configured.");
}
else
{
    try
    {
        tableServiceClient = new TableServiceClient(tableConnectionString);
    }
    catch (Exception ex)
    {
        healthStatus.ConfigurationErrors.Add($"AzureTableStorage:ConnectionString is invalid: {ex.Message}");
    }
}

if (tableServiceClient != null)
{
    builder.Services.AddSingleton(tableServiceClient);
    builder.Services.AddSingleton(sp => new ClientTableService(
        sp.GetRequiredService<TableServiceClient>(),
        "Users",
        "ServerConfig",
        "DataItem",
        "Events",
        sp.GetRequiredService<ILogger<ClientTableService>>()));
}

var blobSection = builder.Configuration.GetSection("AzureBlobStorage");
var blobStorageConfig = new BlobStorageConfig
{
    StorageAccount = blobSection["StorageAccount"] ?? "",
    Container = blobSection["Container"] ?? "",
    AccessKey = blobSection["AccessKey"] ?? "",
    PreviewContainer = blobSection["PreviewContainer"] ?? ""
};
builder.Services.AddSingleton(blobStorageConfig);

StorageSharedKeyCredential? blobCredential = null;
BlobServiceClient? blobServiceClient = null;

if (string.IsNullOrEmpty(blobStorageConfig.StorageAccount))
    healthStatus.ConfigurationErrors.Add("AzureBlobStorage:StorageAccount is not configured.");
if (string.IsNullOrEmpty(blobStorageConfig.AccessKey))
    healthStatus.ConfigurationErrors.Add("AzureBlobStorage:AccessKey is not configured.");
if (string.IsNullOrEmpty(blobStorageConfig.Container))
    healthStatus.ConfigurationErrors.Add("AzureBlobStorage:Container is not configured.");
if (string.IsNullOrEmpty(blobStorageConfig.PreviewContainer))
    healthStatus.ConfigurationErrors.Add("AzureBlobStorage:PreviewContainer is not configured.");

if (!string.IsNullOrEmpty(blobStorageConfig.StorageAccount) && !string.IsNullOrEmpty(blobStorageConfig.AccessKey))
{
    try
    {
        blobCredential = new StorageSharedKeyCredential(blobStorageConfig.StorageAccount, blobStorageConfig.AccessKey);
        var blobServiceUri = blobStorageConfig.StorageAccount == "devstoreaccount1"
            ? new Uri($"http://127.0.0.1:10000/{blobStorageConfig.StorageAccount}")
            : new Uri($"https://{blobStorageConfig.StorageAccount}.blob.core.windows.net");
        blobServiceClient = new BlobServiceClient(blobServiceUri, blobCredential);
    }
    catch (Exception ex)
    {
        healthStatus.ConfigurationErrors.Add($"AzureBlobStorage credentials are invalid: {ex.Message}");
    }
}

if (blobCredential != null)
    builder.Services.AddSingleton(blobCredential);
if (blobServiceClient != null)
    builder.Services.AddSingleton(blobServiceClient);

var dataEngineConfig = builder.Configuration.GetSection("DataEngine").Get<DataEngineConfig>();
if (dataEngineConfig == null)
{
    healthStatus.ConfigurationErrors.Add("DataEngine section is not configured in appsettings.");
    dataEngineConfig = new DataEngineConfig();
}
builder.Services.AddSingleton(dataEngineConfig);

builder.Services.AddHttpClient<EngineApiClient>(client =>
{
    client.Timeout = TimeSpan.FromSeconds(120);
});
builder.Services.AddSingleton<EngineMetadataService>();

var app = builder.Build();

var isAzuriteMode = blobStorageConfig.StorageAccount == "devstoreaccount1"
    || (tableConnectionString != null && (
        tableConnectionString.Contains("devstoreaccount1", StringComparison.OrdinalIgnoreCase)
        || tableConnectionString.Contains("UseDevelopmentStorage=true", StringComparison.OrdinalIgnoreCase)));

if (tableServiceClient != null)
{
    try
    {
        var usersTable = tableServiceClient.GetTableClient("Users");
        await usersTable.CreateIfNotExistsAsync();
        var serverConfigTable = tableServiceClient.GetTableClient("ServerConfig");
        await serverConfigTable.CreateIfNotExistsAsync();
        var dataItemTable = tableServiceClient.GetTableClient("DataItem");
        await dataItemTable.CreateIfNotExistsAsync();
        var eventsTable = tableServiceClient.GetTableClient("Events");
        await eventsTable.CreateIfNotExistsAsync();
    }
    catch (Azure.RequestFailedException ex)
    {
        Console.Error.WriteLine("=== Azure Storage initialization failed ===");
        Console.Error.WriteLine($"HTTP Status : {ex.Status}");
        Console.Error.WriteLine($"Error Code  : {ex.ErrorCode}");
        Console.Error.WriteLine($"Message     : {ex.Message}");
        Console.Error.WriteLine($"Stack Trace : {ex.StackTrace}");
        Console.Error.WriteLine($"Full Details: {ex}");
        if (!isAzuriteMode)
        {
            healthStatus.ConfigurationErrors.Add($"Azure Table Storage initialization failed: {ex.Message}");
        }
        else
        {
            throw;
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("=== Storage initialization failed (unexpected error) ===");
        Console.Error.WriteLine(ex.ToString());
        if (!isAzuriteMode)
        {
            healthStatus.ConfigurationErrors.Add($"Azure Table Storage initialization failed: {ex.Message}");
        }
        else
        {
            throw;
        }
    }
}

if (!isAzuriteMode && blobServiceClient != null)
{
    try
    {
        var previewContainer = blobServiceClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
        await previewContainer.CreateIfNotExistsAsync();
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("=== Azure Blob Storage connectivity check failed ===");
        Console.Error.WriteLine(ex.ToString());
        healthStatus.ConfigurationErrors.Add($"Azure Blob Storage connectivity check failed: {ex.Message}");
    }
}

if (!healthStatus.IsHealthy)
{
    Console.Error.WriteLine("=== HttpServer is starting in degraded mode. Configuration issues detected: ===");
    foreach (var error in healthStatus.ConfigurationErrors)
        Console.Error.WriteLine($"  - {error}");

    _ = Task.Run(async () =>
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(60));
        while (await timer.WaitForNextTickAsync())
        {
            Console.Error.WriteLine("=== [Periodic] HttpServer configuration issues: ===");
            foreach (var error in healthStatus.ConfigurationErrors)
                Console.Error.WriteLine($"  - {error}");
        }
    });
}

app.UseStaticFiles();

app.MapGet("/", () => "DataProtectionTool HttpServer is running.");

app.MapGet("/agents/{path}", async (string path, RpcAgentProxy agentProxy, IWebHostEnvironment env) =>
{
    if (!await agentProxy.TryGetAsync(path))
        return Results.NotFound("Agent not found.");

    if (string.IsNullOrEmpty(env.WebRootPath))
        return Results.NotFound("Frontend not built. No wwwroot directory found. Run 'npm run build' in frontend/.");

    var indexPath = Path.Combine(env.WebRootPath, "index.html");
    if (!File.Exists(indexPath))
        return Results.NotFound("Frontend not built. Run 'npm run build' in frontend/.");

    return Results.Content(File.ReadAllText(indexPath), "text/html");
});

app.MapAgentEndpoints();
app.MapTableEndpoints();
app.MapQueryEndpoints();
app.MapBlobEndpoints();
app.MapEngineEndpoints();
app.MapFlowEndpoints();

await app.RunAsync();
