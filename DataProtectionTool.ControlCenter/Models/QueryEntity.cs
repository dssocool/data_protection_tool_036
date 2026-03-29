using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.Models;

public class QueryEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string ConnectionRowKey { get; set; } = "";
    public string QueryText { get; set; } = "";
    public DateTime CreatedAt { get; set; }

    public static string BuildRowKey(string id) => $"query_{id}";
}
