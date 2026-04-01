using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.HttpServer.Models;

public class ConnectionEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string ConnectionType { get; set; } = "SqlServer";
    public string ServerName { get; set; } = "";
    public string Authentication { get; set; } = "";
    public string UserName { get; set; } = "";
    public string Password { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string Encrypt { get; set; } = "";
    public bool TrustServerCertificate { get; set; }
    public DateTime CreatedAt { get; set; }

    public static string BuildRowKey(string id) => $"connection_{id}";
}
