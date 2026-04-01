using System.Text.Json;
using Grpc.Core;
using DataProtectionTool.Contracts;
using DataProtectionTool.RpcServer.Models;

namespace DataProtectionTool.RpcServer.Services;

public class AgentHubService : AgentHub.AgentHubBase
{
    private readonly ILogger<AgentHubService> _logger;
    private readonly AgentRegistry _registry;
    private readonly ClientTableService? _clientTableService;

    public AgentHubService(
        ILogger<AgentHubService> logger,
        AgentRegistry registry,
        ClientTableService? clientTableService = null)
    {
        _logger = logger;
        _registry = registry;
        _clientTableService = clientTableService;
    }

    public override async Task<RegisterResponse> Register(RegisterRequest request, ServerCallContext context)
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

        if (_clientTableService != null)
        {
            try
            {
                await _clientTableService.CreateOrUpdateClientAsync(oid, tid, request.AgentId, userName);
                var partitionKeyForEvents = ClientEntity.BuildPartitionKey(oid, tid);
                _ = _clientTableService.AppendEventAsync(partitionKeyForEvents, "agent_connected",
                    $"Agent {request.AgentId} connected from {context.Peer}");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to update client table for agent {AgentId}", request.AgentId);
            }
        }

        var connectionsJson = "[]";
        var partitionKey = ClientEntity.BuildPartitionKey(oid, tid);
        try
        {
            var connections = _clientTableService != null
                ? await _clientTableService.GetConnectionsAsync(partitionKey)
                : new List<ConnectionEntity>();
            connectionsJson = JsonSerializer.Serialize(connections.Select(c => new
            {
                rowKey = c.RowKey,
                connectionType = c.ConnectionType,
                serverName = c.ServerName,
                authentication = c.Authentication,
                userName = c.UserName,
                password = c.Password,
                databaseName = c.DatabaseName,
                encrypt = c.Encrypt,
                trustServerCertificate = c.TrustServerCertificate,
            }));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch connections for agent {AgentId}", request.AgentId);
        }

        _logger.LogInformation(
            "Agent {AgentId} registered — oid={Oid}, tid={Tid}, path={Path}",
            request.AgentId, oid, tid, path);

        return new RegisterResponse
        {
            Success = true,
            Path = path,
            ConnectionsJson = connectionsJson,
        };
    }

    public override Task<HeartbeatResponse> Heartbeat(HeartbeatRequest request, ServerCallContext context)
    {
        var exists = _registry.TryGet(request.Path, out _);
        if (!exists)
            _logger.LogWarning("Heartbeat from unknown agent path {Path}", request.Path);
        return Task.FromResult(new HeartbeatResponse { Success = exists });
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

    public override async Task<GetConnectionDetailsResponse> GetConnectionDetails(GetConnectionDetailsRequest request, ServerCallContext context)
    {
        if (!_registry.TryGetConnection(request.Path, out _))
            return new GetConnectionDetailsResponse { Success = false, Error = "Agent not registered" };

        if (_clientTableService == null)
            return new GetConnectionDetailsResponse { Success = false, Error = "Table service is not configured" };

        if (!_registry.TryGet(request.Path, out var info) || info == null)
            return new GetConnectionDetailsResponse { Success = false, Error = "Agent not found" };

        var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
        var entity = await _clientTableService.GetConnectionByRowKeyAsync(partitionKey, request.RowKey);

        if (entity == null)
            return new GetConnectionDetailsResponse { Success = false, Error = $"Connection {request.RowKey} not found." };

        return new GetConnectionDetailsResponse
        {
            Success = true,
            RowKey = entity.RowKey,
            ServerName = entity.ServerName,
            Authentication = entity.Authentication,
            UserName = entity.UserName,
            Password = entity.Password,
            DatabaseName = entity.DatabaseName,
            Encrypt = entity.Encrypt,
            TrustServerCertificate = entity.TrustServerCertificate,
        };
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
