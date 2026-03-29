using Grpc.Core;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.Services;

public class AgentHubService : AgentHub.AgentHubBase
{
    private readonly ILogger<AgentHubService> _logger;

    public AgentHubService(ILogger<AgentHubService> logger)
    {
        _logger = logger;
    }

    public override async Task Connect(
        IAsyncStreamReader<AgentMessage> requestStream,
        IServerStreamWriter<ServerMessage> responseStream,
        ServerCallContext context)
    {
        var peer = context.Peer;
        _logger.LogInformation("Agent connected from {Peer}", peer);

        var readTask = Task.Run(async () =>
        {
            await foreach (var message in requestStream.ReadAllAsync(context.CancellationToken))
            {
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

        try
        {
            await readTask;
        }
        catch (OperationCanceledException)
        {
            // Client disconnected
        }

        _logger.LogInformation("Agent disconnected from {Peer}", peer);
    }
}
