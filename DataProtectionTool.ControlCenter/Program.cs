using Azure.Data.Tables;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using DataProtectionTool.ControlCenter.Endpoints;
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
    "Events",
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

builder.Services.AddHttpClient<EngineApiClient>(client =>
{
    client.Timeout = TimeSpan.FromSeconds(120);
});
builder.Services.AddSingleton<EngineMetadataService>();

var app = builder.Build();

var isAzuriteMode = blobStorageConfig.StorageAccount == "devstoreaccount1"
    || tableConnectionString.Contains("devstoreaccount1", StringComparison.OrdinalIgnoreCase)
    || tableConnectionString.Contains("UseDevelopmentStorage=true", StringComparison.OrdinalIgnoreCase);

try
{
    var usersTable = tableServiceClient.GetTableClient("Users");
    await usersTable.CreateIfNotExistsAsync();
    var controlCenterTable = tableServiceClient.GetTableClient("ControlCenter");
    await controlCenterTable.CreateIfNotExistsAsync();
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
        Console.Error.WriteLine("=== Running in Azure Storage mode (not Azurite). Cannot connect to the configured storage account. Exiting. ===");
        Environment.Exit(1);
    }
    throw;
}
catch (Exception ex)
{
    Console.Error.WriteLine("=== Storage initialization failed (unexpected error) ===");
    Console.Error.WriteLine(ex.ToString());
    if (!isAzuriteMode)
    {
        Console.Error.WriteLine("=== Running in Azure Storage mode (not Azurite). Cannot connect to the configured storage account. Exiting. ===");
        Environment.Exit(1);
    }
    throw;
}

if (!isAzuriteMode)
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
        Console.Error.WriteLine("=== Running in Azure Storage mode (not Azurite). Cannot connect to the configured storage account. Exiting. ===");
        Environment.Exit(1);
    }
}

app.UseStaticFiles();

app.MapGrpcService<AgentHubService>();
app.MapGet("/", () => "DataProtectionTool ControlCenter is running.");

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

app.MapAgentEndpoints();
app.MapTableEndpoints();
app.MapQueryEndpoints();
app.MapBlobEndpoints();
app.MapEngineEndpoints();
app.MapFlowEndpoints();

await app.RunAsync();
