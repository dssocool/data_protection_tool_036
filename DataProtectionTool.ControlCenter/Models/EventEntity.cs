using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.Models;

public class EventEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "all_events";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string EventsJson { get; set; } = "[]";
}

public class EventRecord
{
    public DateTime Timestamp { get; set; }
    public string Type { get; set; } = "";
    public string Summary { get; set; } = "";
    public string Detail { get; set; } = "";
}
