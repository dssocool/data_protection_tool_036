using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.Models;

public class DataItemEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string Schema { get; set; } = "";
    public string TableName { get; set; } = "";
    public string ConnectionRowKey { get; set; } = "";

    public static string BuildRowKeyPrefix(string serverName, string dbName) =>
        $"sqlserver_{serverName}_{dbName}_";

    public static string BuildRowKey(string serverName, string dbName, string tableName, string uuid) =>
        $"sqlserver_{serverName}_{dbName}_{tableName}_{uuid}";
}
