using System.Collections.Concurrent;
using System.Threading.Channels;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.RpcServer.Services;

public record AgentInfo(string Oid, string Tid, string AgentId, DateTime ConnectedAt, string UserName = "");

public class AgentConnection
{
    public AgentInfo Info { get; }
    public Channel<ServerMessage> CommandChannel { get; } = Channel.CreateUnbounded<ServerMessage>();

    private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _pending = new();

    public AgentConnection(AgentInfo info)
    {
        Info = info;
    }

    public async Task<string> SendCommandAsync(string type, string payload, TimeSpan timeout)
    {
        var correlationId = Guid.NewGuid().ToString("N");

        var wrappedPayload = System.Text.Json.JsonSerializer.Serialize(new
        {
            correlationId,
            data = payload
        });

        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[correlationId] = tcs;

        try
        {
            await CommandChannel.Writer.WriteAsync(new ServerMessage
            {
                Type = type,
                Payload = wrappedPayload
            });

            using var cts = new CancellationTokenSource(timeout);
            await using var reg = cts.Token.Register(() =>
                tcs.TrySetException(new TimeoutException("Agent did not respond in time.")));

            return await tcs.Task;
        }
        finally
        {
            _pending.TryRemove(correlationId, out _);
        }
    }

    public bool TryCompleteCommand(string correlationId, string payload)
    {
        if (_pending.TryRemove(correlationId, out var tcs))
        {
            tcs.TrySetResult(payload);
            return true;
        }
        return false;
    }

    public void CancelAll()
    {
        CommandChannel.Writer.TryComplete();
        foreach (var kvp in _pending)
        {
            if (_pending.TryRemove(kvp.Key, out var tcs))
                tcs.TrySetCanceled();
        }
    }
}

public class AgentRegistry
{
    private readonly ConcurrentDictionary<string, AgentConnection> _agents = new();

    public string Register(AgentInfo info)
    {
        var path = Guid.NewGuid().ToString("N");
        _agents[path] = new AgentConnection(info);
        return path;
    }

    public bool TryGet(string path, out AgentInfo? info)
    {
        if (_agents.TryGetValue(path, out var conn))
        {
            info = conn.Info;
            return true;
        }
        info = null;
        return false;
    }

    public bool TryGetConnection(string path, out AgentConnection? connection)
    {
        return _agents.TryGetValue(path, out connection);
    }

    public void Remove(string path)
    {
        if (_agents.TryRemove(path, out var conn))
            conn.CancelAll();
    }
}
