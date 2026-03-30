using System.Text.Json;
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

    public async Task<QueryEntity> SaveQueryAsync(
        string partitionKey,
        string connectionRowKey,
        string queryText)
    {
        EnsureTableExists();
        var id = Guid.NewGuid().ToString("N");
        var entity = new QueryEntity
        {
            PartitionKey = partitionKey,
            RowKey = QueryEntity.BuildRowKey(id),
            ConnectionRowKey = connectionRowKey,
            QueryText = queryText,
            CreatedAt = DateTime.UtcNow
        };

        await _tableClient.AddEntityAsync(entity);
        _logger.LogInformation(
            "Saved query — partitionKey={PK}, rowKey={RK}, connectionRowKey={CRK}",
            partitionKey, entity.RowKey, connectionRowKey);
        return entity;
    }

    public async Task<List<QueryEntity>> GetQueriesAsync(string partitionKey, string connectionRowKey)
    {
        EnsureTableExists();
        var queries = new List<QueryEntity>();

        await foreach (var entity in _tableClient.QueryAsync<QueryEntity>(
            e => e.PartitionKey == partitionKey && e.RowKey.CompareTo("query_") >= 0
                                                && e.RowKey.CompareTo("query_~") < 0))
        {
            if (entity.ConnectionRowKey == connectionRowKey)
                queries.Add(entity);
        }

        return queries;
    }

    public async Task<QueryEntity?> GetQueryByRowKeyAsync(string partitionKey, string rowKey)
    {
        EnsureTableExists();
        try
        {
            var response = await _tableClient.GetEntityAsync<QueryEntity>(partitionKey, rowKey);
            return response.Value;
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<TableFormatEntity?> GetTableFormatAsync(
        string partitionKey, string connectionRowKey, string schema, string tableName)
    {
        EnsureTableExists();
        var rowKey = TableFormatEntity.BuildRowKey(connectionRowKey, schema, tableName);
        try
        {
            var response = await _tableClient.GetEntityAsync<TableFormatEntity>(partitionKey, rowKey);
            return response.Value;
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<TableFormatEntity> SaveTableFormatAsync(
        string partitionKey, string connectionRowKey, string schema, string tableName, string fileFormatId)
    {
        EnsureTableExists();
        var entity = new TableFormatEntity
        {
            PartitionKey = partitionKey,
            RowKey = TableFormatEntity.BuildRowKey(connectionRowKey, schema, tableName),
            ConnectionRowKey = connectionRowKey,
            Schema = schema,
            TableName = tableName,
            FileFormatId = fileFormatId,
            CreatedAt = DateTime.UtcNow
        };

        await _tableClient.UpsertEntityAsync(entity);
        _logger.LogInformation(
            "Saved table format — partitionKey={PK}, rowKey={RK}, fileFormatId={FId}",
            partitionKey, entity.RowKey, fileFormatId);
        return entity;
    }

    public async Task AppendEventAsync(string partitionKey, string type, string summary, string detail = "")
    {
        EnsureTableExists();
        var cutoff = DateTime.UtcNow.AddDays(-30);

        List<EventRecord> events;
        EventEntity entity;

        try
        {
            var response = await _tableClient.GetEntityAsync<EventEntity>(partitionKey, "all_events");
            entity = response.Value;
            events = JsonSerializer.Deserialize<List<EventRecord>>(entity.EventsJson) ?? new List<EventRecord>();
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            entity = new EventEntity
            {
                PartitionKey = partitionKey,
                RowKey = "all_events"
            };
            events = new List<EventRecord>();
        }

        events.RemoveAll(e => e.Timestamp < cutoff);

        events.Add(new EventRecord
        {
            Timestamp = DateTime.UtcNow,
            Type = type,
            Summary = summary,
            Detail = detail
        });

        entity.EventsJson = JsonSerializer.Serialize(events);
        await _tableClient.UpsertEntityAsync(entity);
    }

    public async Task<List<EventRecord>> GetEventsAsync(string partitionKey)
    {
        EnsureTableExists();
        try
        {
            var response = await _tableClient.GetEntityAsync<EventEntity>(partitionKey, "all_events");
            return JsonSerializer.Deserialize<List<EventRecord>>(response.Value.EventsJson) ?? new List<EventRecord>();
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return new List<EventRecord>();
        }
    }
}
