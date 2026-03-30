using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.Models;

public class TableFormatEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string ConnectionRowKey { get; set; } = "";
    public string Schema { get; set; } = "";
    public string TableName { get; set; } = "";
    public string FileFormatId { get; set; } = "";
    public DateTime CreatedAt { get; set; }

    public static string BuildRowKey(string connectionRowKey, string schema, string tableName)
        => $"tableformat_{connectionRowKey}_{TableKeyHelper.EscapeKeySegment(schema)}_{TableKeyHelper.EscapeKeySegment(tableName)}";
}
