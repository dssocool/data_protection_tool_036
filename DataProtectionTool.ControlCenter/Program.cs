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

var app = builder.Build();

app.MapGrpcService<AgentHubService>();
app.MapGet("/", () => "DataProtectionTool ControlCenter is running.");

app.Run();
