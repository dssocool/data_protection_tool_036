using System.Text.Json;

namespace DataProtectionTool.ControlCenter.Services;

public class EngineMetadataService
{
    private readonly EngineApiClient _apiClient;
    private readonly ILogger<EngineMetadataService> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public List<JsonElement>? Algorithms { get; private set; }
    public List<JsonElement>? Domains { get; private set; }
    public List<JsonElement>? Frameworks { get; private set; }
    public bool IsLoaded { get; private set; }

    public EngineMetadataService(EngineApiClient apiClient, ILogger<EngineMetadataService> logger)
    {
        _apiClient = apiClient;
        _logger = logger;
    }

    public async Task EnsureLoadedAsync()
    {
        if (IsLoaded) return;

        await _lock.WaitAsync();
        try
        {
            if (IsLoaded) return;

            _logger.LogInformation("Fetching engine metadata (algorithms, domains, frameworks)...");

            Algorithms = await _apiClient.FetchAllPagesAsync($"{_apiClient.BaseUrl}/algorithms");
            Domains = await _apiClient.FetchAllPagesAsync($"{_apiClient.BaseUrl}/domains");
            Frameworks = await _apiClient.FetchAllPagesAsync($"{_apiClient.BaseUrl}/algorithm/frameworks/?include_schema=false");

            IsLoaded = true;

            _logger.LogInformation("Engine metadata loaded: {Algorithms} algorithms, {Domains} domains, {Frameworks} frameworks",
                Algorithms.Count, Domains.Count, Frameworks.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch engine metadata");
            throw;
        }
        finally
        {
            _lock.Release();
        }
    }
}
