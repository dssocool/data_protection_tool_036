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
    private readonly TableClient _eventsTableClient;
    private readonly ILogger<ClientTableService> _logger;
    private bool _tableInitialized;
    private bool _eventsTableInitialized;

    public ClientTableService(TableServiceClient serviceClient, string tableName, string controlCenterTableName, string dataItemTableName, string eventsTableName, ILogger<ClientTableService> logger)
    {
        _tableName = tableName;
        _logger = logger;
        _tableClient = serviceClient.GetTableClient(_tableName);
        _controlCenterTableClient = serviceClient.GetTableClient(controlCenterTableName);
        _dataItemTableClient = serviceClient.GetTableClient(dataItemTableName);
        _eventsTableClient = serviceClient.GetTableClient(eventsTableName);
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

    private void EnsureEventsTableExists()
    {
        if (_eventsTableInitialized) return;
        try
        {
            _eventsTableClient.CreateIfNotExists();
            _eventsTableInitialized = true;
            _logger.LogInformation("Azure Table Storage initialized — events table '{Table}'", _eventsTableClient.Name);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to ensure events table '{Table}' exists; will retry on next call", _eventsTableClient.Name);
            throw;
        }
    }

    public async Task<ClientEntity> CreateOrUpdateClientAsync(string oid, string tid, string agentId, string userName = "")
    {
        EnsureTableExists();
        var partitionKey = ClientEntity.BuildPartitionKey(oid, tid);

        try
        {
            var existing = await _tableClient.GetEntityAsync<ClientEntity>(partitionKey, "profile");
            existing.Value.AgentId = agentId;
            existing.Value.LastConnectedAt = DateTime.UtcNow;
            if (!string.IsNullOrEmpty(userName))
                existing.Value.UserName = userName;
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
                UserName = userName,
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
        EnsureEventsTableExists();
        var now = DateTime.UtcNow;
        var entity = new EventEntity
        {
            PartitionKey = partitionKey,
            RowKey = EventEntity.BuildRowKey(type, now),
            Value = JsonSerializer.Serialize(new { summary, detail })
        };
        await _eventsTableClient.AddEntityAsync(entity);
    }

    public async Task AppendEventAsync(string partitionKey, string type, string id, string summary, string detail = "")
    {
        EnsureEventsTableExists();
        var now = DateTime.UtcNow;
        var entity = new EventEntity
        {
            PartitionKey = partitionKey,
            RowKey = EventEntity.BuildRowKeyWithId(type, id, now),
            Value = JsonSerializer.Serialize(new { summary, detail })
        };
        await _eventsTableClient.AddEntityAsync(entity);
    }

    public async Task<List<EventRecord>> GetEventsAsync(string partitionKey)
    {
        EnsureEventsTableExists();
        var events = new List<EventRecord>();

        await foreach (var entity in _eventsTableClient.QueryAsync<EventEntity>(
            e => e.PartitionKey == partitionKey))
        {
            var rowKey = entity.RowKey;
            var lastUnderscore = rowKey.LastIndexOf('_');
            var type = lastUnderscore > 0 ? rowKey[..lastUnderscore] : rowKey;
            var timestampStr = lastUnderscore > 0 ? rowKey[(lastUnderscore + 1)..] : "";

            DateTime timestamp;
            if (!DateTime.TryParseExact(timestampStr, "yyyyMMddHHmmssfff",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
                    out timestamp))
            {
                timestamp = entity.Timestamp?.UtcDateTime ?? DateTime.UtcNow;
            }

            var summary = "";
            var detail = "";
            try
            {
                using var doc = JsonDocument.Parse(entity.Value);
                if (doc.RootElement.TryGetProperty("summary", out var summaryEl))
                    summary = summaryEl.GetString() ?? "";
                if (doc.RootElement.TryGetProperty("detail", out var detailEl))
                    detail = detailEl.GetString() ?? "";
            }
            catch (JsonException) { }

            events.Add(new EventRecord
            {
                Timestamp = timestamp,
                Type = type,
                Summary = summary,
                Detail = detail
            });
        }

        return events;
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

    public async Task<int> DeleteFlowsAsync(string partitionKey, IEnumerable<string> rowKeys)
    {
        EnsureTableExists();
        var deleted = 0;
        foreach (var rowKey in rowKeys)
        {
            try
            {
                await _tableClient.DeleteEntityAsync(partitionKey, rowKey);
                deleted++;
                _logger.LogInformation("Deleted flow — partitionKey={PK}, rowKey={RK}", partitionKey, rowKey);
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 404)
            {
                _logger.LogWarning("Flow not found for deletion — partitionKey={PK}, rowKey={RK}", partitionKey, rowKey);
            }
        }
        return deleted;
    }
}
