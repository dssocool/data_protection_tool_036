using Microsoft.AspNetCore.Server.Kestrel.Core;
using DataProtectionTool.ControlCenter.Interceptors;
using DataProtectionTool.ControlCenter.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
    });
});

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<SecretValidationInterceptor>();
});

builder.Services.AddSingleton<AgentRegistry>();

var app = builder.Build();

app.MapGrpcService<AgentHubService>();
app.MapGet("/", () => "DataProtectionTool ControlCenter is running.");

app.MapGet("/agents/{path}", (string path, AgentRegistry registry) =>
{
    if (!registry.TryGet(path, out var info) || info is null)
        return Results.NotFound("Agent not found.");

    var html = $"""
        <!DOCTYPE html>
        <html>
        <head><title>Agent Info</title></head>
        <body>
            <h1>Agent Status</h1>
            <table border="1" cellpadding="8" cellspacing="0">
                <tr><td><strong>OID</strong></td><td>{System.Net.WebUtility.HtmlEncode(info.Oid)}</td></tr>
                <tr><td><strong>TID</strong></td><td>{System.Net.WebUtility.HtmlEncode(info.Tid)}</td></tr>
                <tr><td><strong>Agent ID</strong></td><td>{System.Net.WebUtility.HtmlEncode(info.AgentId)}</td></tr>
                <tr><td><strong>Connected At (UTC)</strong></td><td>{info.ConnectedAt:O}</td></tr>
            </table>
        </body>
        </html>
        """;

    return Results.Content(html, "text/html");
});

app.Run();
