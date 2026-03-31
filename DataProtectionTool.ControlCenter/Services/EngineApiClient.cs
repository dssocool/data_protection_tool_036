using System.Net.Http.Headers;
using System.Text.Json;
using DataProtectionTool.ControlCenter.Models;

namespace DataProtectionTool.ControlCenter.Services;

public class EngineApiClient
{
    private readonly HttpClient _httpClient;
    private readonly DataEngineConfig _config;
    private readonly ILogger<EngineApiClient> _logger;

    public EngineApiClient(HttpClient httpClient, DataEngineConfig config, ILogger<EngineApiClient> logger)
    {
        _httpClient = httpClient;
        _config = config;
        _logger = logger;
    }

    public string BaseUrl => $"{_config.EngineUrl.TrimEnd('/')}/masking/api/v5.1.44";

    public async Task<List<JsonElement>> FetchAllPagesAsync(string url, string? authToken = null)
    {
        var token = authToken ?? _config.AuthorizationToken;
        var allItems = new List<JsonElement>();
        int pageNumber = 1;
        const int maxPages = 100;

        while (pageNumber <= maxPages)
        {
            var separator = url.Contains('?') ? "&" : "?";
            var pagedUrl = $"{url}{separator}page_number={pageNumber}";

            using var request = new HttpRequestMessage(HttpMethod.Get, pagedUrl);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Headers.TryAddWithoutValidation("Authorization", token);

            using var response = await _httpClient.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync();
                _logger.LogWarning("Engine API HTTP {StatusCode} from {Url}: {Body}",
                    (int)response.StatusCode, pagedUrl, errorBody);
                response.EnsureSuccessStatusCode();
            }

            var body = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(body);

            if (!doc.RootElement.TryGetProperty("responseList", out var listEl) || listEl.ValueKind != JsonValueKind.Array)
                break;

            var pageItems = listEl.EnumerateArray().Select(e => e.Clone()).ToList();
            if (pageItems.Count == 0)
                break;

            allItems.AddRange(pageItems);

            if (!doc.RootElement.TryGetProperty("_pageInfo", out var pageInfoEl) || pageInfoEl.ValueKind != JsonValueKind.String)
                break;

            using var pageInfoDoc = JsonDocument.Parse(pageInfoEl.GetString()!);
            var pi = pageInfoDoc.RootElement;
            int numberOnPage = pi.TryGetProperty("numberOnPage", out var nop) ? nop.GetInt32() : 0;
            int total = pi.TryGetProperty("total", out var tot) ? tot.GetInt32() : 0;
            if (numberOnPage >= total)
                break;

            pageNumber++;
        }

        return allItems;
    }

    public async Task<(bool success, string fileFormatId, string responseBody)> CreateFileFormatAsync(
        byte[] fileBytes, string blobFilename, string fileFormatType = "PARQUET")
    {
        using var formContent = new MultipartFormDataContent();
        var fileContent = new ByteArrayContent(fileBytes);
        fileContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        formContent.Add(fileContent, "fileFormat", blobFilename);
        formContent.Add(new StringContent(fileFormatType), "fileFormatType");

        var requestUrl = $"{BaseUrl}/file-formats";
        using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.TryAddWithoutValidation("Authorization", _config.AuthorizationToken);
        request.Content = formContent;

        using var response = await _httpClient.SendAsync(request);
        var responseBody = await response.Content.ReadAsStringAsync();

        string fileFormatId = "";
        try
        {
            using var respDoc = JsonDocument.Parse(responseBody);
            if (respDoc.RootElement.TryGetProperty("fileFormatId", out var ffiEl))
                fileFormatId = ffiEl.ValueKind == JsonValueKind.Number
                    ? ffiEl.GetRawText()
                    : ffiEl.GetString() ?? "";
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse fileFormatId from engine response");
        }

        return (response.IsSuccessStatusCode, fileFormatId, responseBody);
    }

    public async Task<(bool success, string fileRulesetId, string responseBody)> CreateFileRulesetAsync(
        string rulesetName, string fileConnectorId)
    {
        var requestUrl = $"{BaseUrl}/file-rulesets";
        var jsonBody = JsonSerializer.Serialize(new
        {
            rulesetName,
            fileConnectorId = int.Parse(fileConnectorId)
        });

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.TryAddWithoutValidation("Authorization", _config.AuthorizationToken);
        request.Content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");

        using var response = await _httpClient.SendAsync(request);
        var responseBody = await response.Content.ReadAsStringAsync();

        string fileRulesetId = "";
        try
        {
            using var respDoc = JsonDocument.Parse(responseBody);
            if (respDoc.RootElement.TryGetProperty("fileRulesetId", out var friEl))
                fileRulesetId = friEl.ToString();
        }
        catch { }

        return (response.IsSuccessStatusCode, fileRulesetId, responseBody);
    }

    public async Task<(bool success, string fileMetadataId, string responseBody)> CreateFileMetadataAsync(
        string fileName, string rulesetId, string fileFormatId, string fileType = "PARQUET")
    {
        var requestUrl = $"{BaseUrl}/file-metadata";
        var jsonBody = JsonSerializer.Serialize(new
        {
            fileName,
            rulesetId = int.Parse(rulesetId),
            fileFormatId = int.Parse(fileFormatId),
            fileType
        });

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.TryAddWithoutValidation("Authorization", _config.AuthorizationToken);
        request.Content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");

        using var response = await _httpClient.SendAsync(request);
        var responseBody = await response.Content.ReadAsStringAsync();

        string fileMetadataId = "";
        try
        {
            using var respDoc = JsonDocument.Parse(responseBody);
            if (respDoc.RootElement.TryGetProperty("fileMetadataId", out var fmiEl))
                fileMetadataId = fmiEl.ToString();
        }
        catch { }

        return (response.IsSuccessStatusCode, fileMetadataId, responseBody);
    }

    public async Task<List<JsonElement>> FetchColumnRulesAsync(string fileFormatId)
    {
        var url = $"{BaseUrl}/file-field-metadata?file_format_id={Uri.EscapeDataString(fileFormatId)}";
        return await FetchAllPagesAsync(url);
    }

    public async Task<bool> FixColumnRuleAsync(string metadataId)
    {
        var url = $"{BaseUrl}/file-field-metadata/{Uri.EscapeDataString(metadataId)}";
        var jsonBody = JsonSerializer.Serialize(new { isMasked = false, isProfilerWritable = false });

        using var request = new HttpRequestMessage(HttpMethod.Put, url);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.TryAddWithoutValidation("Authorization", _config.AuthorizationToken);
        request.Content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");

        using var response = await _httpClient.SendAsync(request);
        if (!response.IsSuccessStatusCode)
            _logger.LogWarning("Failed to fix column rule {MetadataId}: HTTP {StatusCode}", metadataId, (int)response.StatusCode);
        return response.IsSuccessStatusCode;
    }

    public record ColumnRulesResult(
        List<JsonElement> Rules,
        List<JsonElement> Algorithms,
        List<JsonElement> Domains,
        List<JsonElement> Frameworks);

    public ColumnRulesResult EnrichColumnRules(
        List<JsonElement> rules,
        List<JsonElement>? algorithms,
        List<JsonElement>? domains,
        List<JsonElement>? frameworks)
    {
        var matchedAlgorithms = new Dictionary<string, JsonElement>();
        var matchedDomains = new Dictionary<string, JsonElement>();
        var matchedFrameworks = new Dictionary<string, JsonElement>();

        foreach (var rule in rules)
        {
            if (rule.TryGetProperty("algorithmName", out var algNameEl) && algNameEl.ValueKind == JsonValueKind.String)
            {
                var algName = algNameEl.GetString() ?? "";
                if (!string.IsNullOrEmpty(algName) && !matchedAlgorithms.ContainsKey(algName) && algorithms != null)
                {
                    var match = algorithms.FirstOrDefault(a =>
                        a.TryGetProperty("algorithmName", out var n) && n.GetString() == algName);
                    if (match.ValueKind != JsonValueKind.Undefined)
                    {
                        matchedAlgorithms[algName] = match;

                        if (match.TryGetProperty("frameworkId", out var fwIdEl) && frameworks != null)
                        {
                            var fwIdStr = fwIdEl.ValueKind == JsonValueKind.String ? fwIdEl.GetString() ?? "" : fwIdEl.ToString();
                            if (!string.IsNullOrEmpty(fwIdStr) && !matchedFrameworks.ContainsKey(fwIdStr))
                            {
                                var fwMatch = frameworks.FirstOrDefault(f =>
                                {
                                    if (!f.TryGetProperty("frameworkId", out var fid)) return false;
                                    var fidStr = fid.ValueKind == JsonValueKind.String ? fid.GetString() ?? "" : fid.ToString();
                                    return fidStr == fwIdStr;
                                });
                                if (fwMatch.ValueKind != JsonValueKind.Undefined)
                                    matchedFrameworks[fwIdStr] = fwMatch;
                            }
                        }
                    }
                }
            }

            if (rule.TryGetProperty("domainName", out var domNameEl) && domNameEl.ValueKind == JsonValueKind.String)
            {
                var domName = domNameEl.GetString() ?? "";
                if (!string.IsNullOrEmpty(domName) && !matchedDomains.ContainsKey(domName) && domains != null)
                {
                    var match = domains.FirstOrDefault(d =>
                        d.TryGetProperty("domainName", out var n) && n.GetString() == domName);
                    if (match.ValueKind != JsonValueKind.Undefined)
                        matchedDomains[domName] = match;
                }
            }
        }

        return new ColumnRulesResult(
            rules,
            matchedAlgorithms.Values.ToList(),
            matchedDomains.Values.ToList(),
            matchedFrameworks.Values.ToList());
    }
}
