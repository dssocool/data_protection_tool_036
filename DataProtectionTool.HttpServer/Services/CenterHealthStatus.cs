namespace DataProtectionTool.HttpServer.Services;

public class CenterHealthStatus
{
    public bool IsHealthy => ConfigurationErrors.Count == 0;
    public List<string> ConfigurationErrors { get; } = new();
}
