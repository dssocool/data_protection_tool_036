using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace DataProtectionTool.HttpServer.Helpers;

internal static class EndpointHelpers
{
    private static readonly Regex PreviewFilenameRegex = new(
        "^(?:dryrun_[0-9a-fA-F]{32}_)?(?:preview|fullrun)_(\\d+)_([0-9a-fA-F]{32})(?:_([2-9]\\d*))?\\.parquet$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static async Task<string> ReadBodyAsync(this HttpRequest request)
    {
        using var reader = new StreamReader(request.Body);
        return await reader.ReadToEndAsync();
    }

    public static object EventPayload(string type, string summary, string detail = "")
        => new { timestamp = DateTime.UtcNow.ToString("O"), type, summary, detail };

    public static IResult EventResult(bool success, string message, string eventType, string summary, string detail = "")
        => Results.Ok(new { success, message, @event = EventPayload(eventType, summary, detail) });

    public static IResult EventResultWithRowKey(bool success, string message, string rowKey, string eventType, string summary, string detail = "")
        => Results.Ok(new { success, message, rowKey, @event = EventPayload(eventType, summary, detail) });

    public static string InjectEventIntoResult(string agentResult, string eventType, string summary, string detail = "")
    {
        var evtJson = JsonSerializer.Serialize(EventPayload(eventType, summary, detail));
        return agentResult.TrimEnd().TrimEnd('}') + $",\"event\":{evtJson}}}";
    }

    public static bool IsDigitsOnly(string value) => value.All(char.IsDigit);

    public static bool IsValidPreviewFilename(string filename) => PreviewFilenameRegex.IsMatch(filename);

    public static string AddFieldsToPayload(string body, object fields)
    {
        JsonNode? payloadNode;
        try
        {
            payloadNode = JsonNode.Parse(body);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Invalid request payload: {ex.Message}", ex);
        }

        if (payloadNode is not JsonObject payloadObject)
            throw new InvalidOperationException("Invalid request payload.");

        var fieldsJson = JsonSerializer.SerializeToNode(fields);
        if (fieldsJson is JsonObject fieldsObject)
        {
            foreach (var prop in fieldsObject)
            {
                payloadObject[prop.Key] = prop.Value?.DeepClone();
            }
        }

        return payloadObject.ToJsonString();
    }

    public static List<string> GetAllowedAlgorithmTypes(string sqlServerType)
    {
        var numericSqlTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "int", "bigint", "smallint", "tinyint", "float", "real",
            "decimal", "numeric", "money", "smallmoney", "bit"
        };

        if (numericSqlTypes.Contains(sqlServerType))
            return new List<string> { "BIG_DECIMAL" };

        return new List<string>
        {
            "BIG_DECIMAL", "LOCAL_DATE_TIME", "STRING", "BYTE_BUFFER", "GENERIC_DATA_ROW"
        };
    }
}
