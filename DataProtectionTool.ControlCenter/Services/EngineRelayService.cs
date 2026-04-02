using System.Text.Json;
using DataProtectionTool.ControlCenter.Models;

namespace DataProtectionTool.ControlCenter.Services;

public static class EngineRelayService
{
    public static async Task<JsonDocument> RelayHttpAsync(
        AgentConnection connection, string engineBaseUrl, string authorizationToken,
        string method, string relativeUrl, object? requestBody = null)
    {
        var httpPayload = JsonSerializer.Serialize(new
        {
            method,
            url = $"{engineBaseUrl}/{relativeUrl}",
            headers = new Dictionary<string, string>
            {
                ["accept"] = "application/json",
                ["Authorization"] = authorizationToken,
                ["Content-Type"] = "application/json"
            },
            body = requestBody != null ? JsonSerializer.Serialize(requestBody) : null
        });
        var result = await connection.SendCommandAsync("http_request", httpPayload, TimeSpan.FromSeconds(120));
        return JsonDocument.Parse(result);
    }

    public static string ExtractBodyField(JsonDocument relayResponse, string fieldName)
    {
        if (!relayResponse.RootElement.TryGetProperty("body", out var bodyEl))
            return "";
        using var bodyDoc = JsonDocument.Parse(bodyEl.GetString() ?? "{}");
        return bodyDoc.RootElement.TryGetProperty(fieldName, out var valEl) ? valEl.ToString() : "";
    }

    public static async Task<string> PollExecutionAsync(
        AgentConnection connection, string engineBaseUrl, string authorizationToken,
        string executionId, HttpResponse response, string statusLabel, int maxIterations = 300,
        List<string>? statusSteps = null)
    {
        var status = "";
        for (var i = 0; i < maxIterations; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await RelayHttpAsync(connection, engineBaseUrl, authorizationToken, "GET", $"executions/{executionId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            status = ExtractBodyField(statusResp, "status");
            if (!string.IsNullOrEmpty(statusLabel))
            {
                var msg = $"Polling {statusLabel}: {status}...";
                await SseWriter.WriteEventAsync(response, "status", msg);
                statusSteps?.Add(msg);
            }
            if (status is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        return status;
    }

    public static async Task<bool> ValidateEngineConfigAsync(
        DataEngineConfig config, HttpResponse response, bool requireProfileSetId = false)
    {
        if (string.IsNullOrEmpty(config.EngineUrl) || string.IsNullOrEmpty(config.AuthorizationToken))
        {
            await SseWriter.WriteErrorAsync(response, "Data engine is not configured. Set EngineUrl and AuthorizationToken in appsettings.json.");
            return false;
        }

        if (string.IsNullOrEmpty(config.ConnectorId))
        {
            await SseWriter.WriteErrorAsync(response, "Data engine ConnectorId is not configured. Set ConnectorId in appsettings.json.");
            return false;
        }

        if (requireProfileSetId && string.IsNullOrEmpty(config.ProfileSetId))
        {
            await SseWriter.WriteErrorAsync(response, "Data engine ProfileSetId is not configured. Set ProfileSetId in appsettings.json.");
            return false;
        }

        return true;
    }

    public static async Task<(bool success, List<string> metadataIds)> CreateFileMetadataBatchAsync(
        EngineApiClient engineApi, HttpResponse response,
        List<string> filenames, string fileRulesetId, string fileFormatId,
        List<string>? statusSteps = null)
    {
        var fileMetadataIds = new List<string>();
        for (var fi = 0; fi < filenames.Count; fi++)
        {
            var file = filenames[fi];
            var msg = $"Creating file metadata... ({fi + 1} of {filenames.Count})";
            await SseWriter.WriteEventAsync(response, "status", msg);
            statusSteps?.Add(msg);

            var (metaSuccess, fileMetadataId, _) = await engineApi.CreateFileMetadataAsync(file, fileRulesetId, fileFormatId);
            if (!metaSuccess)
            {
                await SseWriter.WriteErrorAsync(response, $"File metadata creation failed for {file}.");
                return (false, fileMetadataIds);
            }

            fileMetadataIds.Add(fileMetadataId);
        }

        return (true, fileMetadataIds);
    }

    public static async Task WriteStatusThreadSafeAsync(
        HttpResponse response, SemaphoreSlim sseLock, List<string> statusSteps, string msg)
    {
        await sseLock.WaitAsync();
        try
        {
            await SseWriter.WriteEventAsync(response, "status", msg);
            statusSteps.Add(msg);
        }
        finally
        {
            sseLock.Release();
        }
    }

    public static async Task<string> PollExecutionThreadSafeAsync(
        AgentConnection connection, string engineBaseUrl, string authorizationToken,
        string executionId, HttpResponse response, SemaphoreSlim sseLock,
        string statusLabel, int maxIterations = 300, List<string>? statusSteps = null)
    {
        var status = "";
        for (var i = 0; i < maxIterations; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await RelayHttpAsync(connection, engineBaseUrl, authorizationToken, "GET", $"executions/{executionId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            status = ExtractBodyField(statusResp, "status");
            if (!string.IsNullOrEmpty(statusLabel))
            {
                var msg = $"Polling {statusLabel}: {status}...";
                await sseLock.WaitAsync();
                try
                {
                    await SseWriter.WriteEventAsync(response, "status", msg);
                    statusSteps?.Add(msg);
                }
                finally
                {
                    sseLock.Release();
                }
            }
            if (status is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        return status;
    }
}
