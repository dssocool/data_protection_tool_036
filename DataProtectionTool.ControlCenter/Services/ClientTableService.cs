using Azure.Data.Tables;
using DataProtectionTool.ControlCenter.Models;

namespace DataProtectionTool.ControlCenter.Services;

public class ClientTableService
{
    private const string TableName = "Clients";
    private readonly TableClient _tableClient;
    private readonly ILogger<ClientTableService> _logger;
    private bool _tableInitialized;

    public ClientTableService(TableServiceClient serviceClient, ILogger<ClientTableService> logger)
    {
        _logger = logger;
        _tableClient = serviceClient.GetTableClient(TableName);
    }

    private void EnsureTableExists()
    {
        if (_tableInitialized) return;
        try
        {
            _tableClient.CreateIfNotExists();
            _tableInitialized = true;
            _logger.LogInformation("Azure Table Storage initialized — table '{Table}'", TableName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to ensure table '{Table}' exists; will retry on next call", TableName);
            throw;
        }
    }

    public async Task<ClientEntity> CreateOrUpdateClientAsync(string oid, string tid, string agentId)
    {
        EnsureTableExists();
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
        EnsureTableExists();
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

    public async Task<ConnectionEntity> SaveConnectionAsync(
        string partitionKey,
        string serverName,
        string authentication,
        string userName,
        string password,
        string databaseName,
        string encrypt,
        bool trustServerCertificate)
    {
        EnsureTableExists();
        var id = Guid.NewGuid().ToString("N");
        var entity = new ConnectionEntity
        {
            PartitionKey = partitionKey,
            RowKey = ConnectionEntity.BuildRowKey(id),
            ServerName = serverName,
            Authentication = authentication,
            UserName = userName,
            Password = password,
            DatabaseName = databaseName,
            Encrypt = encrypt,
            TrustServerCertificate = trustServerCertificate,
            CreatedAt = DateTime.UtcNow
        };

        await _tableClient.AddEntityAsync(entity);
        _logger.LogInformation(
            "Saved connection — partitionKey={PK}, rowKey={RK}",
            partitionKey, entity.RowKey);
        return entity;
    }

    public async Task<List<ConnectionEntity>> GetConnectionsAsync(string partitionKey)
    {
        EnsureTableExists();
        var connections = new List<ConnectionEntity>();

        await foreach (var entity in _tableClient.QueryAsync<ConnectionEntity>(
            e => e.PartitionKey == partitionKey && e.RowKey.CompareTo("connection_") >= 0
                                                && e.RowKey.CompareTo("connection_~") < 0))
        {
            connections.Add(entity);
        }

        return connections;
    }

    public async Task<ConnectionEntity?> GetConnectionByRowKeyAsync(string partitionKey, string rowKey)
    {
        EnsureTableExists();
        try
        {
            var response = await _tableClient.GetEntityAsync<ConnectionEntity>(partitionKey, rowKey);
            return response.Value;
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }
}
