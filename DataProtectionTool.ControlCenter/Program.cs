using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using DataProtectionTool.ControlCenter.Interceptors;
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

app.Run();
