using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.HttpServer.Models;

public class ClientEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "";
    public string RowKey { get; set; } = "profile";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string Oid { get; set; } = "";
    public string Tid { get; set; } = "";
    public string AgentId { get; set; } = "";
    public string UserName { get; set; } = "";
    public DateTime FirstConnectedAt { get; set; }
    public DateTime LastConnectedAt { get; set; }

    public static string BuildPartitionKey(string oid, string tid) => $"{oid}_{tid}";
}
