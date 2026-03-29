using Azure.Data.Tables;
using DataProtectionTool.ControlCenter.Models;

namespace DataProtectionTool.ControlCenter.Services;

public class ClientTableService
{
    private const string TableName = "Clients";
    private readonly TableClient _tableClient;
    private readonly ILogger<ClientTableService> _logger;

    public ClientTableService(TableServiceClient serviceClient, ILogger<ClientTableService> logger)
    {
        _logger = logger;
        _tableClient = serviceClient.GetTableClient(TableName);
        _tableClient.CreateIfNotExists();
        _logger.LogInformation("Azure Table Storage initialized — table '{Table}'", TableName);
    }

    public async Task<ClientEntity> CreateOrUpdateClientAsync(string oid, string tid, string agentId)
    {
        var partitionKey = ClientEntity.BuildPartitionKey(oid, tid);

        try
        {
            var existing = await _tableClient.GetEntityAsync<ClientEntity>(partitionKey, "profile");
            existing.Value.AgentId = agentId;
            existing.Value.LastConnectedAt = DateTime.UtcNow;
            await _tableClient.UpdateEntityAsync(existing.Value, existing.Value.ETag);
            _logger.LogInformation(
                "Updated existing client — oid={Oid}, tid={Tid}", oid, tid);
            return existing.Value;
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            var entity = new ClientEntity
            {
                PartitionKey = partitionKey,
                RowKey = "profile",
                Oid = oid,
                Tid = tid,
                AgentId = agentId,
                FirstConnectedAt = DateTime.UtcNow,
                LastConnectedAt = DateTime.UtcNow
            };
            await _tableClient.AddEntityAsync(entity);
            _logger.LogInformation(
                "Created new client — oid={Oid}, tid={Tid}", oid, tid);
            return entity;
        }
    }

    public async Task<ClientEntity?> GetClientAsync(string oid, string tid)
    {
        var partitionKey = ClientEntity.BuildPartitionKey(oid, tid);
        try
        {
            var response = await _tableClient.GetEntityAsync<ClientEntity>(partitionKey, "profile");
            return response.Value;
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }
}
