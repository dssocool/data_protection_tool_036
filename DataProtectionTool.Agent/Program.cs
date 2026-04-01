using System.Collections.Concurrent;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Identity;
using Azure.Storage.Blobs;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Data.SqlClient;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using DataProtectionTool.Contracts;

var testMode = args.Any(a => a.Equals("test", StringComparison.OrdinalIgnoreCase));

string oid;
string tid;
string userName;

if (testMode)
{
    oid = Environment.UserName;
    tid = GetLocalIpAddress();
    userName = Environment.UserName;
    Console.WriteLine($"[TEST MODE] Using OS user as oid: {oid}");
    Console.WriteLine($"[TEST MODE] Using local IP as tid: {tid}");
}
else
{
    Console.WriteLine("Authenticating with Azure Identity (DefaultAzureCredential)...");
    var credential = new DefaultAzureCredential();
    var tokenResult = await credential.GetTokenAsync(
        new Azure.Core.TokenRequestContext(new[] { "https://graph.microsoft.com/.default" }));

    var handler = new JwtSecurityTokenHandler();
    var jwt = handler.ReadJwtToken(tokenResult.Token);

    oid = jwt.Claims.FirstOrDefault(c => c.Type == "oid")?.Value
        ?? throw new InvalidOperationException("Token does not contain an 'oid' claim.");
    tid = jwt.Claims.FirstOrDefault(c => c.Type == "tid")?.Value
        ?? throw new InvalidOperationException("Token does not contain a 'tid' claim.");
    userName = jwt.Claims.FirstOrDefault(c => c.Type == "name")?.Value ?? "";

    Console.WriteLine($"Authenticated. oid={oid}, tid={tid}, userName={userName}");
}

var agentId = $"agent-{Environment.MachineName}-{Process.GetCurrentProcess().Id}";
var serverAddress = Environment.GetEnvironmentVariable("GRPC_SERVER_ADDRESS")
    ?? args.FirstOrDefault(a => a.StartsWith("--server="))?.Substring("--server=".Length)
    ?? "http://localhost:8191";

Console.WriteLine($"DataProtectionTool Agent [{agentId}]");

var headers = new Metadata
{
    { SharedSecret.MetadataKey, SharedSecret.Value },
    { SharedSecret.OidMetadataKey, oid },
    { SharedSecret.TidMetadataKey, tid },
    { SharedSecret.UserNameMetadataKey, userName }
};

var connectionManager = new SqlConnectionManager();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

while (!cts.Token.IsCancellationRequested)
{
    Console.WriteLine($"Connecting to Server at {serverAddress}...");

    try
    {
        using var channel = GrpcChannel.ForAddress(serverAddress);
        var client = new AgentHub.AgentHubClient(channel);

        var registerResponse = await client.RegisterAsync(new RegisterRequest
        {
            AgentId = agentId,
            Oid = oid,
            Tid = tid,
            UserName = userName
        }, headers: headers, cancellationToken: cts.Token);

        if (!registerResponse.Success)
        {
            Console.Error.WriteLine($"[Server Error] Registration failed: {registerResponse.Error}");
            break;
        }

        var path = registerResponse.Path;
        var url = registerResponse.Url;
        if (!string.IsNullOrEmpty(url))
            Console.WriteLine($"Agent registered. URL: {url}");
        else
            Console.WriteLine($"Agent registered. Path: {path}");

        connectionManager.LoadConnectionDetails(registerResponse.ConnectionsJson);
        Console.WriteLine("[Agent] Loaded connection details from Server.");

        var ctx = new AgentContext
        {
            Client = client,
            Headers = headers,
            Path = path,
            AgentId = agentId,
            Oid = oid,
            Tid = tid,
            UserName = userName
        };

        var receiveTask = Task.Run(async () =>
        {
            using var call = client.Subscribe(new SubscribeRequest { Path = path },
                headers: headers, cancellationToken: cts.Token);

            Console.WriteLine("Subscribed for server commands.");

            await foreach (var msg in call.ResponseStream.ReadAllAsync(cts.Token))
            {
                HandleServerMessage(ctx, connectionManager, msg);
            }
        });

        var heartbeatTask = Task.Run(async () =>
        {
            var sequence = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await client.HeartbeatAsync(new HeartbeatRequest { Path = path },
                        headers: headers, cancellationToken: cts.Token);
                    Console.WriteLine($"[Agent] Sent heartbeat seq={sequence++}");
                }
                catch (RpcException ex) when (ex.StatusCode != StatusCode.Unauthenticated)
                {
                    Console.Error.WriteLine($"[Agent] Heartbeat failed: {ex.Status.Detail}");
                }
                await Task.Delay(TimeSpan.FromSeconds(60), cts.Token);
            }
        });

        await Task.WhenAny(receiveTask, heartbeatTask);
        await receiveTask;
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.Unauthenticated)
    {
        Console.Error.WriteLine($"Authentication failed: {ex.Status.Detail}");
        break;
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Agent shutting down...");
        break;
    }
    catch (RpcException ex)
    {
        Console.Error.WriteLine($"gRPC error: {ex.Status.StatusCode} — {ex.Status.Detail}");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Unexpected error: {ex.Message}");
    }

    if (!cts.Token.IsCancellationRequested)
    {
        Console.WriteLine("Retrying connection in 30 seconds...");
        try { await Task.Delay(TimeSpan.FromSeconds(30), cts.Token); }
        catch (OperationCanceledException) { break; }
    }
}

connectionManager.Dispose();
Console.WriteLine("Agent disconnected.");

// --- Message dispatch ---

static void HandleServerMessage(AgentContext ctx, SqlConnectionManager connectionManager, ServerMessage msg)
{
    switch (msg.Type)
    {
        case "connections_list":
            connectionManager.LoadConnectionDetails(msg.Payload);
            Console.WriteLine("[Agent] Loaded connection details from Server.");
            break;
        case "validate_sql":
            _ = RunCommandAsync(ctx, msg.Payload, "validate_sql_result", HandleValidateSqlCore);
            break;
        case "execute_sql":
            _ = RunCommandAsync(ctx, msg.Payload, "execute_sql_result",
                (cid, root) => HandleExecuteSqlCore(cid, root, ctx, connectionManager));
            break;
        case "sample_table":
            _ = RunCommandAsync(ctx, msg.Payload, "sample_table_result",
                (cid, root) => HandleSqlToParquetCore(cid, root, ctx, connectionManager));
            break;
        case "validate_query":
            _ = RunCommandAsync(ctx, msg.Payload, "validate_query_result",
                (cid, root) => HandleValidateQueryCore(cid, root, ctx, connectionManager));
            break;
        case "sample_query":
            _ = RunCommandAsync(ctx, msg.Payload, "sample_query_result",
                (cid, root) => HandleSqlToParquetCore(cid, root, ctx, connectionManager,
                    connectionKeyProperty: "connectionRowKey"));
            break;
        case "http_request":
            _ = RunCommandAsync(ctx, msg.Payload, "http_request_result", HandleHttpRequestCore);
            break;
        case "export_table":
            _ = RunCommandAsync(ctx, msg.Payload, "export_table_result",
                (cid, root) => HandleSqlToParquetCore(cid, root, ctx, connectionManager,
                    unlimitedTimeout: true, readFilePrefix: true, readContainerName: true));
            break;
        case "load_masked_to_table":
            _ = RunCommandAsync(ctx, msg.Payload, "load_masked_to_table_result",
                (cid, root) => HandleLoadMaskedToTableCore(cid, root, ctx, connectionManager));
            break;
        default:
            Console.WriteLine($"[Server] Unknown type={msg.Type}, payload={msg.Payload}");
            break;
    }
}

// --- Command runner ---

static async Task RunCommandAsync(
    AgentContext ctx, string payload, string resultType,
    Func<string, JsonElement, Task<string>> coreLogic)
{
    string correlationId = "";
    try
    {
        using var envelope = JsonDocument.Parse(payload);
        correlationId = envelope.RootElement.GetProperty("correlationId").GetString() ?? "";
        var dataJson = envelope.RootElement.GetProperty("data").GetString() ?? "{}";

        using var paramsDoc = JsonDocument.Parse(dataJson);
        var resultJson = await coreLogic(correlationId, paramsDoc.RootElement);

        await ctx.SendResultAsync(resultType, resultJson);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] {resultType} failed: {ex.Message}");
        var errorPayload = JsonSerializer.Serialize(
            new CommandErrorResult(correlationId, false, ex.Message),
            AgentJsonContext.Default.CommandErrorResult);
        try { await ctx.SendResultAsync(resultType, errorPayload); }
        catch (Exception writeEx) { Console.Error.WriteLine($"[Agent] Failed to send error result: {writeEx.Message}"); }
    }
}

// --- Handlers ---

static async Task<string> HandleValidateSqlCore(string correlationId, JsonElement root)
{
    var details = ConnectionDetails.FromJson(root);
    if (string.IsNullOrEmpty(details.Encrypt)) details.Encrypt = "Mandatory";
    if (string.IsNullOrEmpty(details.Authentication)) details.Authentication = "Microsoft Entra Integrated";

    Console.WriteLine($"[Agent] Testing SQL connection to {details.ServerName}...");

    await using var conn = SqlConnectionManager.BuildSqlConnection(details);
    await conn.OpenAsync();

    Console.WriteLine("[Agent] SQL connection test succeeded.");
    return JsonSerializer.Serialize(
        new ValidateSqlResult(correlationId, true, $"Connection successful. Server version: {conn.ServerVersion}"),
        AgentJsonContext.Default.ValidateSqlResult);
}

static async Task<string> HandleExecuteSqlCore(
    string correlationId, JsonElement root,
    AgentContext ctx, SqlConnectionManager connManager)
{
    var rowKey = root.GetProperty("rowKey").GetString() ?? "";
    var sqlStatement = root.GetProperty("sqlStatement").GetString() ?? "";

    Console.WriteLine($"[Agent] Executing SQL on connection {rowKey}...");

    var rows = await connManager.ExecuteWithRetryAsync(rowKey, ctx,
        async (sqlConn) =>
        {
            var result = new List<Dictionary<string, object?>>();
            await using var cmd = sqlConn.CreateCommand();
            cmd.CommandText = sqlStatement;

            if (root.TryGetProperty("sqlParams", out var sqlParamsEl) &&
                sqlParamsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var param in sqlParamsEl.EnumerateObject())
                    cmd.Parameters.AddWithValue(param.Name, param.Value.GetString() ?? "");
            }

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                result.Add(row);
            }
            return result;
        });

    Console.WriteLine($"[Agent] Executed SQL, returned {rows.Count} rows for connection {rowKey}.");
    return JsonSerializer.Serialize(
        new ExecuteSqlResult(correlationId, true, rows),
        AgentJsonContext.Default.ExecuteSqlResult);
}

static async Task<string> HandleSqlToParquetCore(
    string correlationId, JsonElement root,
    AgentContext ctx, SqlConnectionManager connManager,
    string connectionKeyProperty = "rowKey",
    bool unlimitedTimeout = false,
    bool readFilePrefix = false,
    bool readContainerName = false)
{
    var rowKey = root.GetProperty(connectionKeyProperty).GetString() ?? "";
    var uniqueId = root.GetProperty("uniqueId").GetString() ?? "";
    var sqlStatement = root.GetProperty("sqlStatement").GetString() ?? "";
    var filePrefix = readFilePrefix && root.TryGetProperty("filePrefix", out var fpEl)
        ? fpEl.GetString() ?? "preview" : "preview";
    var containerName = readContainerName && root.TryGetProperty("containerName", out var cnEl)
        ? cnEl.GetString() : null;

    if (string.IsNullOrWhiteSpace(uniqueId))
        throw new InvalidOperationException("Missing uniqueId in request.");

    Console.WriteLine($"[Agent] SQL->Parquet for connection {rowKey} (timeout={unlimitedTimeout})...");

    var filenames = await connManager.ExecuteWithRetryAsync(rowKey, ctx,
        async (sqlConn) =>
        {
            await using var cmd = sqlConn.CreateCommand();
            cmd.CommandText = sqlStatement;
            if (unlimitedTimeout) cmd.CommandTimeout = 0;

            await using var reader = await cmd.ExecuteReaderAsync();
            return await StreamReaderToParquetBlobs(reader, ctx, uniqueId, filePrefix, containerName);
        });

    Console.WriteLine($"[Agent] Uploaded {filenames.Count} Parquet file(s) for connection {rowKey}");
    return JsonSerializer.Serialize(
        new SqlToParquetResult(correlationId, true, filenames),
        AgentJsonContext.Default.SqlToParquetResult);
}

static async Task<string> HandleValidateQueryCore(
    string correlationId, JsonElement root,
    AgentContext ctx, SqlConnectionManager connManager)
{
    var connectionRowKey = root.GetProperty("connectionRowKey").GetString() ?? "";
    var queryText = root.GetProperty("queryText").GetString() ?? "";
    var sqlStatementBefore = root.GetProperty("sqlStatementBefore").GetString() ?? "";
    var sqlStatementAfter = root.GetProperty("sqlStatementAfter").GetString() ?? "";

    Console.WriteLine($"[Agent] Validating query for connection {connectionRowKey}...");

    var message = await connManager.ExecuteWithRetryAsync(connectionRowKey, ctx,
        async (sqlConn) =>
        {
            await using var cmdBefore = sqlConn.CreateCommand();
            cmdBefore.CommandText = sqlStatementBefore;
            await cmdBefore.ExecuteNonQueryAsync();

            try
            {
                await using var cmdQuery = sqlConn.CreateCommand();
                cmdQuery.CommandText = queryText;
                await cmdQuery.ExecuteNonQueryAsync();
                return "Query syntax is valid.";
            }
            finally
            {
                await using var cmdAfter = sqlConn.CreateCommand();
                cmdAfter.CommandText = sqlStatementAfter;
                await cmdAfter.ExecuteNonQueryAsync();
            }
        });

    Console.WriteLine("[Agent] Query validation succeeded.");
    return JsonSerializer.Serialize(
        new ValidateQueryResult(correlationId, true, message),
        AgentJsonContext.Default.ValidateQueryResult);
}

static async Task<string> HandleLoadMaskedToTableCore(
    string correlationId, JsonElement root,
    AgentContext ctx, SqlConnectionManager connManager)
{
    var destRowKey = root.GetProperty("destRowKey").GetString() ?? "";
    var destSchema = root.GetProperty("destSchema").GetString() ?? "";
    var tableName = root.GetProperty("tableName").GetString() ?? "";
    var blobFilename = root.GetProperty("blobFilename").GetString() ?? "";
    var createTable = root.TryGetProperty("createTable", out var ctEl) && ctEl.GetBoolean();
    var truncate = root.TryGetProperty("truncate", out var trEl) && trEl.GetBoolean();
    var containerName = root.TryGetProperty("containerName", out var cnEl) ? cnEl.GetString() : null;

    Console.WriteLine($"[Agent] Loading masked file {blobFilename} -> [{destSchema}].[{tableName}] (create={createTable}, truncate={truncate})");

    var sasInfo = await ctx.GetSasTokenAsync(containerName);
    var blobUri = new Uri($"{sasInfo.BlobEndpoint}/{sasInfo.Container}/{blobFilename}?{sasInfo.SasToken}");
    var blobClient = new BlobClient(blobUri);

    var tempFile = Path.GetTempFileName();
    try
    {
        await blobClient.DownloadToAsync(tempFile);

        using var parquetReader = await ParquetReader.CreateAsync(tempFile);

        var parquetFields = parquetReader.Schema.GetDataFields();
        var columnNames = parquetFields.Select(f => f.Name).ToArray();

        string[] sqlTypes;
        if (parquetReader.CustomMetadata != null &&
            parquetReader.CustomMetadata.TryGetValue("sql_types", out var sqlTypesJson))
        {
            sqlTypes = JsonSerializer.Deserialize(sqlTypesJson, AgentJsonContext.Default.StringArray) ?? columnNames.Select(_ => "nvarchar(max)").ToArray();
        }
        else
        {
            sqlTypes = columnNames.Select(_ => "nvarchar(max)").ToArray();
        }

        await connManager.ExecuteWithRetryAsync(destRowKey, ctx,
            async (sqlConn) =>
            {
                if (createTable)
                {
                    var columnDefs = new StringBuilder();
                    for (int i = 0; i < columnNames.Length; i++)
                    {
                        if (i > 0) columnDefs.Append(", ");
                        columnDefs.Append($"[{columnNames[i]}] {sqlTypes[i]} NULL");
                    }

                    var createSql = $@"IF OBJECT_ID('[{destSchema}].[{tableName}]', 'U') IS NULL
                            CREATE TABLE [{destSchema}].[{tableName}] ({columnDefs})";
                    await using var createCmd = sqlConn.CreateCommand();
                    createCmd.CommandText = createSql;
                    await createCmd.ExecuteNonQueryAsync();
                    Console.WriteLine($"[Agent] Ensured table [{destSchema}].[{tableName}] exists.");
                }

                if (truncate)
                {
                    try
                    {
                        await using var truncCmd = sqlConn.CreateCommand();
                        truncCmd.CommandText = $"TRUNCATE TABLE [{destSchema}].[{tableName}]";
                        await truncCmd.ExecuteNonQueryAsync();
                    }
                    catch (SqlException)
                    {
                        await using var delCmd = sqlConn.CreateCommand();
                        delCmd.CommandText = $"DELETE FROM [{destSchema}].[{tableName}]";
                        await delCmd.ExecuteNonQueryAsync();
                    }
                    Console.WriteLine($"[Agent] Truncated [{destSchema}].[{tableName}].");
                }

                using var bulkCopy = new SqlBulkCopy(sqlConn)
                {
                    DestinationTableName = $"[{destSchema}].[{tableName}]",
                    BulkCopyTimeout = 0
                };

                for (int i = 0; i < columnNames.Length; i++)
                    bulkCopy.ColumnMappings.Add(columnNames[i], columnNames[i]);

                for (int g = 0; g < parquetReader.RowGroupCount; g++)
                {
                    using var rowGroupReader = parquetReader.OpenRowGroupReader(g);
                    var columns = new DataColumn[columnNames.Length];
                    for (int i = 0; i < columnNames.Length; i++)
                        columns[i] = await rowGroupReader.ReadColumnAsync(parquetFields[i]);

                    var rowCount = columns[0].Data.Length;
                    var dataTable = new System.Data.DataTable();
                    for (int i = 0; i < columnNames.Length; i++)
                        dataTable.Columns.Add(columnNames[i], typeof(string));

                    for (int r = 0; r < rowCount; r++)
                    {
                        var row = dataTable.NewRow();
                        for (int c = 0; c < columnNames.Length; c++)
                        {
                            var val = columns[c].Data.GetValue(r);
                            row[c] = val ?? DBNull.Value;
                        }
                        dataTable.Rows.Add(row);
                    }

                    await bulkCopy.WriteToServerAsync(dataTable);
                    Console.WriteLine($"[Agent] Bulk-copied {rowCount} row(s) from row group {g + 1}/{parquetReader.RowGroupCount}");
                }

                return true;
            });
    }
    finally
    {
        try { File.Delete(tempFile); } catch { }
    }

    return JsonSerializer.Serialize(
        new LoadMaskedResult(correlationId, true),
        AgentJsonContext.Default.LoadMaskedResult);
}

static async Task<string> HandleHttpRequestCore(string correlationId, JsonElement root)
{
    var method = root.GetProperty("method").GetString() ?? "GET";
    var url = root.GetProperty("url").GetString() ?? "";
    var bodyContent = root.TryGetProperty("body", out var bodyEl) ? bodyEl.GetString() : null;

    Console.WriteLine($"[Agent] Relaying HTTP {method} {url}...");

    using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(100) };
    var requestMessage = new HttpRequestMessage(new HttpMethod(method), url);

    if (root.TryGetProperty("headers", out var headersEl) && headersEl.ValueKind == JsonValueKind.Object)
    {
        foreach (var header in headersEl.EnumerateObject())
        {
            var headerValue = header.Value.GetString() ?? "";
            if (!requestMessage.Headers.TryAddWithoutValidation(header.Name, headerValue))
            {
                requestMessage.Content ??= new StringContent(bodyContent ?? "");
                requestMessage.Content.Headers.Remove(header.Name);
                requestMessage.Content.Headers.TryAddWithoutValidation(header.Name, headerValue);
            }
        }
    }

    if (bodyContent != null && requestMessage.Content == null)
        requestMessage.Content = new StringContent(bodyContent, Encoding.UTF8);

    using var response = await httpClient.SendAsync(requestMessage);
    var responseBody = await response.Content.ReadAsStringAsync();

    var responseHeaders = CollectHeaders(response.Headers, response.Content.Headers);

    Console.WriteLine($"[Agent] HTTP relay completed: {(int)response.StatusCode} {response.StatusCode}");

    if (!response.IsSuccessStatusCode)
    {
        Console.WriteLine($"[Agent] *** HTTP request failed ***");
        Console.WriteLine($"[Agent]   Request : {method} {url}");
        foreach (var h in requestMessage.Headers)
            Console.WriteLine($"[Agent]   Request Header : {h.Key}: {string.Join(", ", h.Value)}");
        if (requestMessage.Content != null)
            foreach (var h in requestMessage.Content.Headers)
                Console.WriteLine($"[Agent]   Request Header : {h.Key}: {string.Join(", ", h.Value)}");
        if (bodyContent != null)
        {
            var bodyPreview = bodyContent.Length > 2000
                ? bodyContent[..2000] + $"... (truncated, total {bodyContent.Length} chars)"
                : bodyContent;
            Console.WriteLine($"[Agent]   Request Body : {bodyPreview}");
        }
        Console.WriteLine($"[Agent]   Response Status : {(int)response.StatusCode} {response.ReasonPhrase}");
        foreach (var h in responseHeaders)
            Console.WriteLine($"[Agent]   Response Header: {h.Key}: {h.Value}");
        var respPreview = responseBody.Length > 4000
            ? responseBody[..4000] + $"... (truncated, total {responseBody.Length} chars)"
            : responseBody;
        Console.WriteLine($"[Agent]   Response Body  : {respPreview}");
        Console.WriteLine($"[Agent] *** End of failed HTTP details ***");

        var requestHeaders = CollectHeaders(requestMessage.Headers,
            requestMessage.Content?.Headers);

        return JsonSerializer.Serialize(
            new HttpFailResult(correlationId, false,
                $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}",
                (int)response.StatusCode, responseHeaders, responseBody,
                new HttpRequestDetails(method, url, requestHeaders, bodyContent)),
            AgentJsonContext.Default.HttpFailResult);
    }

    return JsonSerializer.Serialize(
        new HttpSuccessResult(correlationId, true, (int)response.StatusCode, responseHeaders, responseBody),
        AgentJsonContext.Default.HttpSuccessResult);
}

static Dictionary<string, string> CollectHeaders(
    System.Net.Http.Headers.HttpHeaders primary,
    System.Net.Http.Headers.HttpHeaders? content = null)
{
    var dict = new Dictionary<string, string>();
    foreach (var h in primary)
        dict[h.Key] = string.Join(", ", h.Value);
    if (content != null)
        foreach (var h in content)
            dict[h.Key] = string.Join(", ", h.Value);
    return dict;
}

// --- Parquet streaming ---

const int PreviewBatchSize = 10_000;

static async Task<List<string>> StreamReaderToParquetBlobs(
    SqlDataReader reader, AgentContext ctx, string uniqueId,
    string filePrefix = "preview",
    string? containerName = null)
{
    var columnCount = reader.FieldCount;
    var columnNames = new string[columnCount];
    var sqlTypes = new string[columnCount];
    var dataFields = new DataField[columnCount];

    for (int i = 0; i < columnCount; i++)
    {
        columnNames[i] = reader.GetName(i);
        sqlTypes[i] = reader.GetDataTypeName(i);
        dataFields[i] = new DataField(columnNames[i], typeof(string), isNullable: true);
    }

    var sqlTypesMetadata = new Dictionary<string, string>
    {
        ["sql_types"] = JsonSerializer.Serialize(sqlTypes, AgentJsonContext.Default.StringArray)
    };

    var parquetSchema = new ParquetSchema(dataFields);
    var sasInfo = await ctx.GetSasTokenAsync(containerName);
    var filenames = new List<string>();
    var previewRequestUuid = Guid.NewGuid().ToString("N");
    var fileSequence = 1;
    bool hasMoreRows = true;

    while (hasMoreRows)
    {
        var columnData = new List<object?>[columnCount];
        for (int i = 0; i < columnCount; i++)
            columnData[i] = new List<object?>();

        int rowsInBatch = 0;
        while (rowsInBatch < PreviewBatchSize && (hasMoreRows = await reader.ReadAsync()))
        {
            for (int i = 0; i < columnCount; i++)
            {
                if (reader.IsDBNull(i))
                    columnData[i].Add(null);
                else
                    columnData[i].Add(reader.GetValue(i)?.ToString() ?? "");
            }
            rowsInBatch++;
        }

        if (rowsInBatch == 0 && filenames.Count > 0)
            break;

        using var ms = new MemoryStream();
        using (var writer = await ParquetWriter.CreateAsync(parquetSchema, ms))
        {
            writer.CustomMetadata = sqlTypesMetadata;
            using var rowGroup = writer.CreateRowGroup();
            for (int i = 0; i < columnCount; i++)
            {
                var column = new DataColumn(dataFields[i], columnData[i].Select(v => (string?)v).ToArray());
                await rowGroup.WriteColumnAsync(column);
            }
        }

        var fname = await UploadParquetBlobAsync(ms, sasInfo, uniqueId, previewRequestUuid, fileSequence, filePrefix);
        filenames.Add(fname);
        fileSequence++;

        Console.WriteLine($"[Agent] Uploaded batch Parquet ({rowsInBatch} rows): {fname}");
    }

    if (filenames.Count == 0)
    {
        using var ms = new MemoryStream();
        using (var writer = await ParquetWriter.CreateAsync(parquetSchema, ms))
        {
            using var rowGroup = writer.CreateRowGroup();
            for (int i = 0; i < columnCount; i++)
            {
                var column = new DataColumn(dataFields[i], Array.Empty<string?>());
                await rowGroup.WriteColumnAsync(column);
            }
        }

        var fname = await UploadParquetBlobAsync(ms, sasInfo, uniqueId, previewRequestUuid, fileSequence, filePrefix);
        filenames.Add(fname);

        Console.WriteLine($"[Agent] Uploaded empty preview Parquet: {fname}");
    }

    return filenames;
}

static async Task<string> UploadParquetBlobAsync(
    MemoryStream ms, SasTokenInfo sasInfo,
    string uniqueId, string previewUuid, int fileSequence, string filePrefix)
{
    var filename = fileSequence <= 1
        ? $"{filePrefix}_{uniqueId}_{previewUuid}.parquet"
        : $"{filePrefix}_{uniqueId}_{previewUuid}_{fileSequence}.parquet";

    var blobUri = new Uri($"{sasInfo.BlobEndpoint}/{sasInfo.Container}/{filename}?{sasInfo.SasToken}");
    var blobClient = new BlobClient(blobUri);
    ms.Position = 0;
    await blobClient.UploadAsync(ms, overwrite: true);
    return filename;
}

// --- Utility ---

static string GetLocalIpAddress()
{
    try
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.Connect("8.8.8.8", 80);
        if (socket.LocalEndPoint is IPEndPoint endPoint)
            return endPoint.Address.ToString();
    }
    catch
    {
    }

    var host = Dns.GetHostEntry(Dns.GetHostName());
    var ipv4 = host.AddressList.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork);
    return ipv4?.ToString() ?? "127.0.0.1";
}

// --- Data types and managers ---

class AgentContext
{
    public required AgentHub.AgentHubClient Client { get; init; }
    public required Metadata Headers { get; init; }
    public required string Path { get; init; }
    public required string AgentId { get; init; }
    public required string Oid { get; init; }
    public required string Tid { get; init; }
    public required string UserName { get; init; }

    public async Task SendResultAsync(string type, string payload)
    {
        await Client.SendCommandResultAsync(new SendCommandResultRequest
        {
            Path = Path,
            Type = type,
            Payload = payload
        }, headers: Headers);
    }

    public async Task<SasTokenInfo> GetSasTokenAsync(string? containerName = null)
    {
        var response = await Client.GetSasTokenAsync(new GetSasTokenRequest
        {
            Path = Path,
            ContainerName = containerName ?? ""
        }, headers: Headers);

        if (!response.Success)
            throw new InvalidOperationException($"Failed to get SAS token: {response.Error}");

        return new SasTokenInfo
        {
            SasToken = response.SasToken,
            BlobEndpoint = response.BlobEndpoint,
            Container = response.Container
        };
    }

    public async Task<ConnectionDetails> GetConnectionDetailsAsync(string rowKey)
    {
        var response = await Client.GetConnectionDetailsAsync(new GetConnectionDetailsRequest
        {
            Path = Path,
            RowKey = rowKey
        }, headers: Headers);

        if (!response.Success)
            throw new InvalidOperationException($"Failed to get connection details: {response.Error}");

        return new ConnectionDetails
        {
            RowKey = response.RowKey,
            ServerName = response.ServerName,
            Authentication = response.Authentication,
            UserName = response.UserName,
            Password = response.Password,
            DatabaseName = response.DatabaseName,
            Encrypt = response.Encrypt,
            TrustServerCertificate = response.TrustServerCertificate,
        };
    }
}

class ConnectionDetails
{
    public string RowKey { get; set; } = "";
    public string ServerName { get; set; } = "";
    public string Authentication { get; set; } = "";
    public string UserName { get; set; } = "";
    public string Password { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string Encrypt { get; set; } = "";
    public bool TrustServerCertificate { get; set; }

    public static ConnectionDetails FromJson(JsonElement el) => new()
    {
        RowKey = el.TryGetProperty("rowKey", out var rk) ? rk.GetString() ?? "" : "",
        ServerName = el.TryGetProperty("serverName", out var sn) ? sn.GetString() ?? "" : "",
        Authentication = el.TryGetProperty("authentication", out var au) ? au.GetString() ?? "" : "",
        UserName = el.TryGetProperty("userName", out var un) ? un.GetString() ?? "" : "",
        Password = el.TryGetProperty("password", out var pw) ? pw.GetString() ?? "" : "",
        DatabaseName = el.TryGetProperty("databaseName", out var db) ? db.GetString() ?? "" : "",
        Encrypt = el.TryGetProperty("encrypt", out var en) ? en.GetString() ?? "" : "",
        TrustServerCertificate = el.TryGetProperty("trustServerCertificate", out var tsc) && tsc.GetBoolean(),
    };
}

class SqlConnectionManager : IDisposable
{
    private readonly ConcurrentDictionary<string, ConnectionDetails> _details = new();
    private readonly ConcurrentDictionary<string, SqlConnection> _connections = new();

    public void LoadConnectionDetails(string connectionsListJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(connectionsListJson);
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                var details = ConnectionDetails.FromJson(el);
                if (!string.IsNullOrEmpty(details.RowKey))
                {
                    _details[details.RowKey] = details;
                    Console.WriteLine($"[ConnMgr] Stored details for {details.RowKey} ({details.ServerName})");
                }
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ConnMgr] Failed to load connection details: {ex.Message}");
        }
    }

    public async Task<SqlConnection> GetOrCreateConnectionAsync(string rowKey)
    {
        if (_connections.TryGetValue(rowKey, out var existing) &&
            existing.State == System.Data.ConnectionState.Open)
        {
            return existing;
        }

        if (!_details.TryGetValue(rowKey, out var details))
            throw new InvalidOperationException($"No connection details found for {rowKey}");

        var conn = BuildSqlConnection(details);
        await conn.OpenAsync();

        if (_connections.TryRemove(rowKey, out var old))
        {
            try { await old.DisposeAsync(); } catch { }
        }

        _connections[rowKey] = conn;
        Console.WriteLine($"[ConnMgr] Opened SQL connection for {rowKey} ({details.ServerName})");
        return conn;
    }

    public async Task<T> ExecuteWithRetryAsync<T>(
        string rowKey, AgentContext ctx,
        Func<SqlConnection, Task<T>> operation)
    {
        try
        {
            var conn = await GetOrCreateConnectionAsync(rowKey);
            return await operation(conn);
        }
        catch (Exception ex) when (ex is SqlException || ex is InvalidOperationException)
        {
            Console.WriteLine($"[ConnMgr] Error for {rowKey}, attempting reconnect: {ex.Message}");

            EvictConnection(rowKey);

            try
            {
                var refreshed = await ctx.GetConnectionDetailsAsync(rowKey);
                _details[refreshed.RowKey] = refreshed;
                Console.WriteLine($"[ConnMgr] Refreshed connection details for {rowKey}");
            }
            catch (Exception refetchEx)
            {
                Console.Error.WriteLine($"[ConnMgr] Failed to re-fetch details for {rowKey}: {refetchEx.Message}");
            }

            var conn = await GetOrCreateConnectionAsync(rowKey);
            return await operation(conn);
        }
    }

    private void EvictConnection(string rowKey)
    {
        if (_connections.TryRemove(rowKey, out var old))
        {
            try { old.Dispose(); } catch { }
        }
    }

    internal static SqlConnection BuildSqlConnection(ConnectionDetails details)
    {
        var csb = new SqlConnectionStringBuilder
        {
            DataSource = details.ServerName,
            Encrypt = details.Encrypt == "Mandatory" ? SqlConnectionEncryptOption.Mandatory
                    : details.Encrypt == "Strict" ? SqlConnectionEncryptOption.Strict
                    : SqlConnectionEncryptOption.Optional,
            TrustServerCertificate = details.TrustServerCertificate,
        };

        if (!string.IsNullOrEmpty(details.DatabaseName))
            csb.InitialCatalog = details.DatabaseName;

        if (details.Authentication == "Microsoft Entra Integrated")
        {
            csb.Authentication = SqlAuthenticationMethod.ActiveDirectoryIntegrated;
        }
        else
        {
            csb.UserID = details.UserName;
            csb.Password = details.Password;
        }

        return new SqlConnection(csb.ConnectionString);
    }

    public void Dispose()
    {
        foreach (var kvp in _connections)
        {
            try { kvp.Value.Dispose(); } catch { }
        }
        _connections.Clear();
    }
}

record CommandErrorResult(string correlationId, bool success, string message);
record ValidateSqlResult(string correlationId, bool success, string message);
record ExecuteSqlResult(string correlationId, bool success, List<Dictionary<string, object?>> rows);
record SqlToParquetResult(string correlationId, bool success, List<string> filenames);
record ValidateQueryResult(string correlationId, bool success, string message);
record LoadMaskedResult(string correlationId, bool success);
record HttpSuccessResult(string correlationId, bool success, int statusCode, Dictionary<string, string> headers, string body);
record HttpFailResult(string correlationId, bool success, string message, int statusCode, Dictionary<string, string> headers, string body, HttpRequestDetails requestDetails);
record HttpRequestDetails(string method, string url, Dictionary<string, string> headers, string? body);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(CommandErrorResult))]
[JsonSerializable(typeof(ValidateSqlResult))]
[JsonSerializable(typeof(ExecuteSqlResult))]
[JsonSerializable(typeof(SqlToParquetResult))]
[JsonSerializable(typeof(ValidateQueryResult))]
[JsonSerializable(typeof(LoadMaskedResult))]
[JsonSerializable(typeof(HttpSuccessResult))]
[JsonSerializable(typeof(HttpFailResult))]
internal partial class AgentJsonContext : JsonSerializerContext { }

class SasTokenInfo
{
    public string SasToken { get; set; } = "";
    public string BlobEndpoint { get; set; } = "";
    public string Container { get; set; } = "";
}
