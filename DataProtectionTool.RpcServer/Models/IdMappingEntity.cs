using Azure;
using Azure.Data.Tables;

namespace DataProtectionTool.ControlCenter.RpcServer.Models;

public class IdMappingEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "id_to_user";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string Value { get; set; } = "";
}
