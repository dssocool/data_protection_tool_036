using System.Text.Json;
using Azure.Data.Tables;
using DataProtectionTool.ControlCenter.Models;

namespace DataProtectionTool.ControlCenter.Services;

public class ClientTableService
{
    private readonly string _tableName;
    private readonly TableClient _tableClient;
    private readonly TableClient _controlCenterTableClient;
    private readonly TableClient _dataItemTableClient;
    private readonly ILogger<ClientTableService> _logger;
    private bool _tableInitialized;

    public ClientTableService(TableServiceClient serviceClient, string tableName, string controlCenterTableName, string dataItemTableName, ILogger<ClientTableService> logger)
    {
        _tableName = tableName;
        _logger = logger;
        _tableClient = serviceClient.GetTableClient(_tableName);
        _controlCenterTableClient = serviceClient.GetTableClient(controlCenterTableName);
        _dataItemTableClient = serviceClient.GetTableClient(dataItemTableName);
    }

    private void EnsureTableExists()
    {
        if (_tableInitialized) return;
        try
        {
            _tableClient.CreateIfNotExists();
            _tableInitialized = true;
            _logger.LogInformation("Azure Table Storage initialized — table '{Table}'", _tableName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to ensure table '{Table}' exists; will retry on next call", _tableName);
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

            var uniqueId = await AssignUserIdAsync(partitionKey);
            _logger.LogInformation(
                "Created new client — oid={Oid}, tid={Tid}, uniqueId={UniqueId}", oid, tid, uniqueId);
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

    public async Task<int> GetNextUserIdAsync()
    {
        int maxId = 0;

        await foreach (var entity in _controlCenterTableClient.QueryAsync<IdMappingEntity>(
            e => e.PartitionKey == "id_to_user"))
        {
            if (int.TryParse(entity.RowKey, out var id) && id > maxId)
                maxId = id;
        }

        return maxId + 1;
    }

    public async Task<int> AssignUserIdAsync(string userPartitionKey)
    {
        var nextId = await GetNextUserIdAsync();
        var idStr = nextId.ToString();

        var idMapping = new IdMappingEntity
        {
            PartitionKey = "id_to_user",
            RowKey = idStr,
            Value = userPartitionKey
        };
        await _controlCenterTableClient.AddEntityAsync(idMapping);

        var uniqueId = new UniqueIdEntity
        {
            PartitionKey = userPartitionKey,
            RowKey = "unique_id",
            Value = idStr
        };
        await _tableClient.AddEntityAsync(uniqueId);

        _logger.LogInformation(
            "Assigned unique ID {UniqueId} to user {UserPartitionKey}", idStr, userPartitionKey);

        return nextId;
    }

    public async Task<string?> GetUserIdAsync(string partitionKey)
    {
        EnsureTableExists();
        try
        {
            var response = await _tableClient.GetEntityAsync<UniqueIdEntity>(partitionKey, "unique_id");
            return response.Value.Value;
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<List<DataItemEntity>> GetDataItemsAsync(string partitionKey, string serverName, string dbName)
    {
        var prefix = DataItemEntity.BuildRowKeyPrefix(serverName, dbName);
        var items = new List<DataItemEntity>();

        await foreach (var entity in _dataItemTableClient.QueryAsync<DataItemEntity>(
            e => e.PartitionKey == partitionKey
                 && e.RowKey.CompareTo(prefix) >= 0
                 && e.RowKey.CompareTo(prefix + "~") < 0))
        {
            items.Add(entity);
        }

        return items;
    }

    public async Task SaveDataItemsAsync(
        string partitionKey, string serverName, string dbName, string connectionRowKey,
        List<(string schema, string name)> tables)
    {
        var prefix = DataItemEntity.BuildRowKeyPrefix(serverName, dbName);

        var existing = new List<DataItemEntity>();
        await foreach (var entity in _dataItemTableClient.QueryAsync<DataItemEntity>(
            e => e.PartitionKey == partitionKey
                 && e.RowKey.CompareTo(prefix) >= 0
                 && e.RowKey.CompareTo(prefix + "~") < 0))
        {
            existing.Add(entity);
        }

        foreach (var old in existing)
        {
            await _dataItemTableClient.DeleteEntityAsync(old.PartitionKey, old.RowKey);
        }

        foreach (var (schema, name) in tables)
        {
            var uuid = Guid.NewGuid().ToString("N");
            var entity = new DataItemEntity
            {
                PartitionKey = partitionKey,
                RowKey = DataItemEntity.BuildRowKey(serverName, dbName, $"{schema}.{name}", uuid),
                ServerName = serverName,
                DatabaseName = dbName,
                Schema = schema,
                TableName = name,
                ConnectionRowKey = connectionRowKey
            };
            await _dataItemTableClient.AddEntityAsync(entity);
        }

        _logger.LogInformation(
            "Saved {Count} data items — partitionKey={PK}, server={Server}, db={Db}",
            tables.Count, partitionKey, serverName, dbName);
    }

    public async Task<DataItemEntity?> GetDataItemByTableAsync(
        string partitionKey, string serverName, string dbName, string schema, string tableName)
    {
        var prefix = DataItemEntity.BuildRowKeyPrefix(serverName, dbName);
        var fullTableName = $"{schema}.{tableName}";

        await foreach (var entity in _dataItemTableClient.QueryAsync<DataItemEntity>(
            e => e.PartitionKey == partitionKey
                 && e.RowKey.CompareTo(prefix) >= 0
                 && e.RowKey.CompareTo(prefix + "~") < 0))
        {
            if (entity.Schema == schema && entity.TableName == tableName)
                return entity;
        }

        return null;
    }

    public async Task UpdatePreviewFileListAsync(DataItemEntity entity, string previewFileList)
    {
        entity.PreviewFileList = previewFileList;
        await _dataItemTableClient.UpdateEntityAsync(entity, entity.ETag);
        _logger.LogInformation(
            "Updated PreviewFileList for DataItem {RowKey} — {FileCount} file(s)",
            entity.RowKey, string.IsNullOrEmpty(previewFileList) ? 0 : previewFileList.Split(',').Length);
    }

    public async Task UpdateFileFormatIdAsync(DataItemEntity entity, string fileFormatId)
    {
        entity.FileFormatId = fileFormatId;
        await _dataItemTableClient.UpdateEntityAsync(entity, entity.ETag);
        _logger.LogInformation(
            "Updated FileFormatId for DataItem {RowKey} — fileFormatId={FileFormatId}",
            entity.RowKey, fileFormatId);
    }

    public async Task<FlowEntity> SaveFlowAsync(string partitionKey, string sourceJson, string destJson)
    {
        EnsureTableExists();
        var id = Guid.NewGuid().ToString("N");
        var entity = new FlowEntity
        {
            PartitionKey = partitionKey,
            RowKey = FlowEntity.BuildRowKey(id),
            SourceJson = sourceJson,
            DestJson = destJson,
            CreatedAt = DateTime.UtcNow
        };

        await _tableClient.AddEntityAsync(entity);
        _logger.LogInformation(
            "Saved flow — partitionKey={PK}, rowKey={RK}",
            partitionKey, entity.RowKey);
        return entity;
    }

    public async Task<List<FlowEntity>> GetFlowsAsync(string partitionKey)
    {
        EnsureTableExists();
        var flows = new List<FlowEntity>();

        await foreach (var entity in _tableClient.QueryAsync<FlowEntity>(
            e => e.PartitionKey == partitionKey && e.RowKey.CompareTo("flow_") >= 0
                                                && e.RowKey.CompareTo("flow_~") < 0))
        {
            flows.Add(entity);
        }

        return flows;
    }
}
