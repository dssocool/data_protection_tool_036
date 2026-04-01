using DataProtectionTool.RpcServer.Interceptors;
using DataProtectionTool.RpcServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<SecretValidationInterceptor>();
});

builder.Services.AddSingleton<AgentRegistry>();

var app = builder.Build();

app.MapGrpcService<AgentHubService>();
app.MapGrpcService<ControlPlaneService>();
app.MapGet("/", () => "DataProtectionTool RpcServer is running.");

await app.RunAsync();
