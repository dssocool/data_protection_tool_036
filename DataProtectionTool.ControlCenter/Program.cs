using Azure.Data.Tables;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using DataProtectionTool.ControlCenter.Interceptors;
using DataProtectionTool.ControlCenter.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(6000, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
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

var app = builder.Build();

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

app.MapGet("/agents/{path}", (string path, AgentRegistry registry, IWebHostEnvironment env) =>
{
    if (!registry.TryGet(path, out _))
        return Results.NotFound("Agent not found.");

    var indexPath = Path.Combine(env.WebRootPath, "index.html");
    if (!File.Exists(indexPath))
        return Results.NotFound("Frontend not built. Run 'npm run build' in frontend/.");

    return Results.Content(File.ReadAllText(indexPath), "text/html");
});

app.Run();
