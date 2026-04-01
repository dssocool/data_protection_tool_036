using Azure.Data.Tables;
using Azure.Storage;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using DataProtectionTool.ControlCenter.RpcServer.Interceptors;
using DataProtectionTool.ControlCenter.RpcServer.Models;
using DataProtectionTool.ControlCenter.RpcServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8191, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<SecretValidationInterceptor>();
});

var healthStatus = new CenterHealthStatus();
builder.Services.AddSingleton(healthStatus);
builder.Services.AddSingleton<AgentRegistry>();

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
        "ControlCenter",
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
    }
    catch (Exception ex)
    {
        healthStatus.ConfigurationErrors.Add($"AzureBlobStorage credentials are invalid: {ex.Message}");
    }
}

if (blobCredential != null)
    builder.Services.AddSingleton(blobCredential);

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

if (!healthStatus.IsHealthy)
{
    Console.Error.WriteLine("=== RpcServer is starting in degraded mode. Configuration issues detected: ===");
    foreach (var error in healthStatus.ConfigurationErrors)
        Console.Error.WriteLine($"  - {error}");

    _ = Task.Run(async () =>
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
        while (await timer.WaitForNextTickAsync())
        {
            Console.Error.WriteLine("=== [Periodic] RpcServer configuration issues: ===");
            foreach (var error in healthStatus.ConfigurationErrors)
                Console.Error.WriteLine($"  - {error}");
        }
    });
}

app.MapGrpcService<AgentHubService>();
app.MapGet("/", () => "DataProtectionTool RpcServer is running.");

await app.RunAsync();
