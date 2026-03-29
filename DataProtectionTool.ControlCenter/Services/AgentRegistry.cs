using System.Collections.Concurrent;

namespace DataProtectionTool.ControlCenter.Services;

public record AgentInfo(string Oid, string Tid, string AgentId, DateTime ConnectedAt);

public class AgentRegistry
{
    private readonly ConcurrentDictionary<string, AgentInfo> _agents = new();

    public string Register(AgentInfo info)
    {
        var path = Guid.NewGuid().ToString("N");
        _agents[path] = info;
        return path;
    }

    public bool TryGet(string path, out AgentInfo? info)
    {
        return _agents.TryGetValue(path, out info);
    }

    public void Remove(string path)
    {
        _agents.TryRemove(path, out _);
    }
}
