using Grpc.Core;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.Services;

public class AgentHubService : AgentHub.AgentHubBase
{
    private readonly ILogger<AgentHubService> _logger;
    private readonly AgentRegistry _registry;
    private readonly ClientTableService _clientTableService;

    public AgentHubService(
        ILogger<AgentHubService> logger,
        AgentRegistry registry,
        ClientTableService clientTableService)
    {
        _logger = logger;
        _registry = registry;
        _clientTableService = clientTableService;
    }

    public override async Task Connect(
        IAsyncStreamReader<AgentMessage> requestStream,
        IServerStreamWriter<ServerMessage> responseStream,
        ServerCallContext context)
    {
        var peer = context.Peer;
        _logger.LogInformation("Agent connected from {Peer}", peer);

        string? registeredPath = null;

        try
        {
            if (!await requestStream.MoveNext(context.CancellationToken))
            {
                _logger.LogWarning("Agent from {Peer} disconnected without sending any messages", peer);
                return;
            }

            var firstMessage = requestStream.Current;

            var oid = firstMessage.Oid;
            var tid = firstMessage.Tid;

            if (string.IsNullOrEmpty(oid))
                oid = context.RequestHeaders.GetValue(SharedSecret.OidMetadataKey) ?? "";
            if (string.IsNullOrEmpty(tid))
                tid = context.RequestHeaders.GetValue(SharedSecret.TidMetadataKey) ?? "";

            var agentInfo = new AgentInfo(oid, tid, firstMessage.AgentId, DateTime.UtcNow);
            registeredPath = _registry.Register(agentInfo);

            await _clientTableService.CreateOrUpdateClientAsync(oid, tid, firstMessage.AgentId);

            var url = $"http://localhost:6000/agents/{registeredPath}";
            _logger.LogInformation(
                "Agent {AgentId} registered — oid={Oid}, tid={Tid}, url={Url}",
                firstMessage.AgentId, oid, tid, url);

            await responseStream.WriteAsync(new ServerMessage
            {
                Type = "registration_url",
                Payload = url
            });

            var readTask = Task.Run(async () =>
            {
                while (await requestStream.MoveNext(context.CancellationToken))
                {
                    var message = requestStream.Current;
                    _logger.LogInformation(
                        "Received from agent {AgentId}: type={Type}, payload={Payload}",
                        message.AgentId, message.Type, message.Payload);

                    var response = new ServerMessage
                    {
                        Type = "ack",
                        Payload = $"Received {message.Type} from {message.AgentId}"
                    };
                    await responseStream.WriteAsync(response);
                }
            });

            await readTask;
        }
        catch (OperationCanceledException)
        {
            // Client disconnected
        }
        finally
        {
            if (registeredPath != null)
                _registry.Remove(registeredPath);
        }

        _logger.LogInformation("Agent disconnected from {Peer}", peer);
    }
}
