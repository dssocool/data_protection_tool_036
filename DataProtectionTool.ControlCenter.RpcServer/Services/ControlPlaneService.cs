using Grpc.Core;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.RpcServer.Services;

public class ControlPlaneService : ControlPlane.ControlPlaneBase
{
    private readonly AgentRegistry _registry;
    private readonly ILogger<ControlPlaneService> _logger;

    public ControlPlaneService(AgentRegistry registry, ILogger<ControlPlaneService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override Task<GetAgentInfoResponse> GetAgentInfo(GetAgentInfoRequest request, ServerCallContext context)
    {
        if (_registry.TryGet(request.Path, out var info) && info is not null)
        {
            return Task.FromResult(new GetAgentInfoResponse
            {
                Found = true,
                Oid = info.Oid,
                Tid = info.Tid,
                AgentId = info.AgentId,
                ConnectedAt = info.ConnectedAt.ToString("O"),
                UserName = info.UserName
            });
        }

        return Task.FromResult(new GetAgentInfoResponse { Found = false });
    }

    public override async Task<SendCommandResponse> SendCommand(SendCommandRequest request, ServerCallContext context)
    {
        if (!_registry.TryGetConnection(request.Path, out var connection) || connection is null)
        {
            return new SendCommandResponse
            {
                Success = false,
                Error = "Agent not found or not connected."
            };
        }

        try
        {
            var timeout = TimeSpan.FromSeconds(request.TimeoutSeconds > 0 ? request.TimeoutSeconds : 30);
            var result = await connection.SendCommandAsync(request.Type, request.Payload, timeout);
            return new SendCommandResponse
            {
                Success = true,
                Payload = result
            };
        }
        catch (TimeoutException)
        {
            return new SendCommandResponse
            {
                Success = false,
                Error = $"Agent did not respond within {request.TimeoutSeconds} seconds."
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SendCommand failed for path={Path}, type={Type}", request.Path, request.Type);
            return new SendCommandResponse
            {
                Success = false,
                Error = ex.Message
            };
        }
    }

    public override async Task<PushConnectionsListResponse> PushConnectionsList(PushConnectionsListRequest request, ServerCallContext context)
    {
        if (!_registry.TryGetConnection(request.Path, out var connection) || connection is null)
        {
            return new PushConnectionsListResponse
            {
                Success = false,
                Error = "Agent not found or not connected."
            };
        }

        try
        {
            await connection.CommandChannel.Writer.WriteAsync(new ServerMessage
            {
                Type = "connections_list",
                Payload = request.ConnectionsJson
            });

            return new PushConnectionsListResponse { Success = true };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PushConnectionsList failed for path={Path}", request.Path);
            return new PushConnectionsListResponse
            {
                Success = false,
                Error = ex.Message
            };
        }
    }
}
