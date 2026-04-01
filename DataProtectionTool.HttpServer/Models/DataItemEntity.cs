using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.HttpServer.Models;

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
    public string PreviewFileList { get; set; } = "";
    public string FileFormatId { get; set; } = "";

    public static string BuildRowKeyPrefix(string serverName, string dbName) =>
        $"sqlserver_{TableKeyHelper.EscapeKeySegment(serverName)}_{TableKeyHelper.EscapeKeySegment(dbName)}_";

    public static string BuildRowKey(string serverName, string dbName, string tableName, string uuid) =>
        $"sqlserver_{TableKeyHelper.EscapeKeySegment(serverName)}_{TableKeyHelper.EscapeKeySegment(dbName)}_{tableName}_{uuid}";
}
