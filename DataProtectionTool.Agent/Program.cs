using System.Diagnostics;
using Grpc.Core;
using Grpc.Net.Client;
using DataProtectionTool.Contracts;

var agentId = $"agent-{Environment.MachineName}-{Process.GetCurrentProcess().Id}";
var serverAddress = "http://localhost:5000";

Console.WriteLine($"DataProtectionTool Agent [{agentId}]");
Console.WriteLine($"Connecting to ControlCenter at {serverAddress}...");

using var channel = GrpcChannel.ForAddress(serverAddress);
var client = new AgentHub.AgentHubClient(channel);

var headers = new Metadata
{
    { SharedSecret.MetadataKey, SharedSecret.Value }
};

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    using var call = client.Connect(headers: headers, cancellationToken: cts.Token);

    Console.WriteLine("Bidirectional stream established.");

    var receiveTask = Task.Run(async () =>
    {
        await foreach (var response in call.ResponseStream.ReadAllAsync(cts.Token))
        {
            Console.WriteLine($"[Server] type={response.Type}, payload={response.Payload}");
        }
    });

    var sendTask = Task.Run(async () =>
    {
        var sequence = 0;
        while (!cts.Token.IsCancellationRequested)
        {
            var message = new AgentMessage
            {
                AgentId = agentId,
                Type = "heartbeat",
                Payload = $"seq={sequence++}"
            };

            await call.RequestStream.WriteAsync(message, cts.Token);
            Console.WriteLine($"[Agent] Sent heartbeat seq={sequence - 1}");
            await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
        }
    });

    await Task.WhenAny(receiveTask, sendTask);

    await call.RequestStream.CompleteAsync();
    await receiveTask;
}
catch (RpcException ex) when (ex.StatusCode == StatusCode.Unauthenticated)
{
    Console.Error.WriteLine($"Authentication failed: {ex.Status.Detail}");
}
catch (OperationCanceledException)
{
    Console.WriteLine("Agent shutting down...");
}
catch (RpcException ex)
{
    Console.Error.WriteLine($"gRPC error: {ex.Status.StatusCode} — {ex.Status.Detail}");
}

Console.WriteLine("Agent disconnected.");
