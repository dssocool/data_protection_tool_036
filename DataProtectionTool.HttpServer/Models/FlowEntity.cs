using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.HttpServer.Models;

public class FlowEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string SourceJson { get; set; } = "";
    public string DestJson { get; set; } = "";
    public DateTime CreatedAt { get; set; }

    public static string BuildRowKey(string id) => $"flow_{id}";
}
