using System.Text.Json;
using Grpc.Core;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.RpcServer.Services;

public class AgentHubService : AgentHub.AgentHubBase
{
    private readonly ILogger<AgentHubService> _logger;
    private readonly AgentRegistry _registry;

    public AgentHubService(ILogger<AgentHubService> logger, AgentRegistry registry)
    {
        _logger = logger;
        _registry = registry;
    }

    public override Task<RegisterResponse> Register(RegisterRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Agent {AgentId} registering from {Peer}", request.AgentId, context.Peer);

        var oid = request.Oid;
        var tid = request.Tid;
        var userName = request.UserName;

        if (string.IsNullOrEmpty(oid))
            oid = context.RequestHeaders.GetValue(SharedSecret.OidMetadataKey) ?? "";
        if (string.IsNullOrEmpty(tid))
            tid = context.RequestHeaders.GetValue(SharedSecret.TidMetadataKey) ?? "";
        if (string.IsNullOrEmpty(userName))
            userName = context.RequestHeaders.GetValue(SharedSecret.UserNameMetadataKey) ?? "";

        var agentInfo = new AgentInfo(oid, tid, request.AgentId, DateTime.UtcNow, userName);
        var path = _registry.Register(agentInfo);

        _logger.LogInformation(
            "Agent {AgentId} registered — oid={Oid}, tid={Tid}, path={Path}",
            request.AgentId, oid, tid, path);

        return Task.FromResult(new RegisterResponse
        {
            Success = true,
            Path = path,
        });
    }

    public override Task<SendCommandResultResponse> SendCommandResult(SendCommandResultRequest request, ServerCallContext context)
    {
        var routed = false;
        if (_registry.TryGetConnection(request.Path, out var conn) && conn != null)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Payload);
                if (doc.RootElement.TryGetProperty("correlationId", out var cidEl))
                {
                    var correlationId = cidEl.GetString();
                    if (!string.IsNullOrEmpty(correlationId))
                        routed = conn.TryCompleteCommand(correlationId, request.Payload);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to route command result from agent at {Path}", request.Path);
            }
        }

        if (!routed)
            _logger.LogWarning("Could not route command result type={Type} from path={Path}", request.Type, request.Path);

        return Task.FromResult(new SendCommandResultResponse { Success = routed });
    }

    public override async Task Subscribe(
        SubscribeRequest request,
        IServerStreamWriter<ServerMessage> responseStream,
        ServerCallContext context)
    {
        if (!_registry.TryGetConnection(request.Path, out var conn) || conn == null)
        {
            _logger.LogWarning("Subscribe from unknown agent path {Path}", request.Path);
            return;
        }

        _logger.LogInformation("Agent at path={Path} subscribed for commands", request.Path);

        try
        {
            await foreach (var msg in conn.CommandChannel.Reader.ReadAllAsync(context.CancellationToken))
            {
                await responseStream.WriteAsync(msg);
            }
        }
        catch (OperationCanceledException)
        {
        }

        _logger.LogInformation("Agent at path={Path} unsubscribed", request.Path);
    }
}
