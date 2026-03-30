using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.Models;

public class UniqueIdEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "unique_id";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string Value { get; set; } = "";
}
