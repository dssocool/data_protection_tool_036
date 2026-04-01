using Azure.Data.Tables;
using DataProtectionTool.RpcServer.Interceptors;
using DataProtectionTool.RpcServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<SecretValidationInterceptor>();
});

builder.Services.AddSingleton<AgentRegistry>();

var tableConnectionString = builder.Configuration.GetSection("AzureTableStorage")["ConnectionString"];
TableServiceClient? tableServiceClient = null;

if (string.IsNullOrEmpty(tableConnectionString))
{
    Console.Error.WriteLine("WARNING: AzureTableStorage:ConnectionString is not configured.");
}
else
{
    try
    {
        tableServiceClient = new TableServiceClient(tableConnectionString);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"WARNING: AzureTableStorage:ConnectionString is invalid: {ex.Message}");
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

var app = builder.Build();

var isAzuriteMode = tableConnectionString != null && (
    tableConnectionString.Contains("devstoreaccount1", StringComparison.OrdinalIgnoreCase)
    || tableConnectionString.Contains("UseDevelopmentStorage=true", StringComparison.OrdinalIgnoreCase));

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
        if (isAzuriteMode)
            throw;
        Console.Error.WriteLine("WARNING: Azure Table Storage initialization failed. Table features will be unavailable.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("=== Storage initialization failed (unexpected error) ===");
        Console.Error.WriteLine(ex.ToString());
        if (isAzuriteMode)
            throw;
        Console.Error.WriteLine("WARNING: Azure Table Storage initialization failed. Table features will be unavailable.");
    }
}

app.MapGrpcService<AgentHubService>();
app.MapGrpcService<ControlPlaneService>();
app.MapGet("/", () => "DataProtectionTool RpcServer is running.");

await app.RunAsync();
