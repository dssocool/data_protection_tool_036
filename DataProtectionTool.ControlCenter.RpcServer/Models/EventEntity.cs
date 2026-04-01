using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.RpcServer.Models;

public class EventEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string Value { get; set; } = "{}";

    public static string BuildRowKey(string eventType, DateTime timestamp)
        => $"{eventType}_{timestamp:yyyyMMddHHmmssfff}";

    public static string BuildRowKeyWithId(string eventType, string id, DateTime timestamp)
        => $"{eventType}_{id}_{timestamp:yyyyMMddHHmmssfff}";
}

public class EventRecord
{
    public DateTime Timestamp { get; set; }
    public string Type { get; set; } = "";
    public string FlowId { get; set; } = "";
    public string Summary { get; set; } = "";
    public string Detail { get; set; } = "";
}
