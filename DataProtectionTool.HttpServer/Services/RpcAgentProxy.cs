using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.HttpServer.Services;

public record AgentInfoDto(string Oid, string Tid, string AgentId, DateTime ConnectedAt, string UserName = "");

public class RpcAgentConnection
{
    private readonly ControlPlane.ControlPlaneClient _client;
    private readonly string _path;

    public AgentInfoDto Info { get; }

    public RpcAgentConnection(ControlPlane.ControlPlaneClient client, string path, AgentInfoDto info)
    {
        _client = client;
        _path = path;
        Info = info;
    }

    public async Task<string> SendCommandAsync(string type, string payload, TimeSpan timeout)
    {
        var response = await _client.SendCommandAsync(new SendCommandRequest
        {
            Path = _path,
            Type = type,
            Payload = payload,
            TimeoutSeconds = (int)timeout.TotalSeconds
        });

        if (!response.Success)
        {
            if (response.Error.Contains("did not respond within"))
                throw new TimeoutException(response.Error);
            throw new InvalidOperationException(response.Error);
        }

        return response.Payload;
    }

    public async Task PushConnectionsListAsync(string connectionsJson)
    {
        var response = await _client.PushConnectionsListAsync(new PushConnectionsListRequest
        {
            Path = _path,
            ConnectionsJson = connectionsJson
        });

        if (!response.Success)
            throw new InvalidOperationException(response.Error);
    }
}

public class RpcAgentProxy
{
    private readonly ControlPlane.ControlPlaneClient _client;

    public RpcAgentProxy(ControlPlane.ControlPlaneClient client)
    {
        _client = client;
    }

    public async Task<bool> TryGetAsync(string path)
    {
        var response = await _client.GetAgentInfoAsync(new GetAgentInfoRequest { Path = path });
        return response.Found;
    }

    public async Task<(bool found, AgentInfoDto? info)> GetAgentInfoAsync(string path)
    {
        var response = await _client.GetAgentInfoAsync(new GetAgentInfoRequest { Path = path });
        if (!response.Found)
            return (false, null);

        var connectedAt = DateTime.TryParse(response.ConnectedAt, out var dt) ? dt : DateTime.UtcNow;
        var info = new AgentInfoDto(response.Oid, response.Tid, response.AgentId, connectedAt, response.UserName);
        return (true, info);
    }

    public async Task<(bool found, RpcAgentConnection? connection)> GetConnectionAsync(string path)
    {
        var (found, info) = await GetAgentInfoAsync(path);
        if (!found || info is null)
            return (false, null);

        return (true, new RpcAgentConnection(_client, path, info));
    }
}
