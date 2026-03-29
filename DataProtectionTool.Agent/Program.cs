using System.Collections.Concurrent;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Azure.Identity;
using Azure.Storage.Blobs;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Data.SqlClient;
using DataProtectionTool.Contracts;

var testMode = args.Any(a => a.Equals("test", StringComparison.OrdinalIgnoreCase));

string oid;
string tid;

if (testMode)
{
    oid = Environment.UserName;
    tid = GetLocalIpAddress();
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

    Console.WriteLine($"Authenticated. oid={oid}, tid={tid}");
}

var agentId = $"agent-{Environment.MachineName}-{Process.GetCurrentProcess().Id}";
var serverAddress = "http://localhost:8191";

Console.WriteLine($"DataProtectionTool Agent [{agentId}]");

var headers = new Metadata
{
    { SharedSecret.MetadataKey, SharedSecret.Value },
    { SharedSecret.OidMetadataKey, oid },
    { SharedSecret.TidMetadataKey, tid }
};

var connectionManager = new SqlConnectionManager();
var sasTokenManager = new SasTokenManager();
DataEngineConfig? dataEngineConfig = null;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

while (!cts.Token.IsCancellationRequested)
{
    Console.WriteLine($"Connecting to ControlCenter at {serverAddress}...");

    try
    {
        using var channel = GrpcChannel.ForAddress(serverAddress);
        var client = new AgentHub.AgentHubClient(channel);

        using var call = client.Connect(headers: headers, cancellationToken: cts.Token);

        Console.WriteLine("Bidirectional stream established.");

        var registerMessage = new AgentMessage
        {
            AgentId = agentId,
            Type = "register",
            Payload = "",
            Oid = oid,
            Tid = tid
        };
        await call.RequestStream.WriteAsync(registerMessage, cts.Token);
        Console.WriteLine("Sent registration message with oid/tid.");

        while (await call.ResponseStream.MoveNext(cts.Token))
        {
            var response = call.ResponseStream.Current;
            if (response.Type == "registration_url")
            {
                Console.WriteLine($"Agent registered. URL: {response.Payload}");
            }
            else if (response.Type == "connections_list")
            {
                connectionManager.LoadConnectionDetails(response.Payload);
                Console.WriteLine("[Agent] Loaded connection details from ControlCenter.");
            }
            else if (response.Type == "data_engine_config")
            {
                dataEngineConfig = JsonSerializer.Deserialize<DataEngineConfig>(response.Payload,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                Console.WriteLine($"[Agent] Loaded data engine config: {dataEngineConfig?.EngineUrl}");
                break;
            }
            else
            {
                Console.WriteLine($"[Server] type={response.Type}, payload={response.Payload}");
                break;
            }
        }

        var receiveTask = Task.Run(async () =>
        {
            while (await call.ResponseStream.MoveNext(cts.Token))
            {
                var response = call.ResponseStream.Current;
                Console.WriteLine($"[Server] type={response.Type}, payload={response.Payload}");

                if (response.Type == "validate_sql")
                {
                    _ = HandleValidateSqlAsync(call, agentId, oid, tid, response.Payload);
                }
                else if (response.Type == "list_tables")
                {
                    _ = HandleListTablesAsync(call, agentId, oid, tid, response.Payload, connectionManager);
                }
                else if (response.Type == "preview_table")
                {
                    _ = HandlePreviewTableAsync(call, agentId, oid, tid, response.Payload, connectionManager, sasTokenManager);
                }
                else if (response.Type == "validate_query")
                {
                    _ = HandleValidateQueryAsync(call, agentId, oid, tid, response.Payload, connectionManager);
                }
                else if (response.Type == "preview_query")
                {
                    _ = HandlePreviewQueryAsync(call, agentId, oid, tid, response.Payload, connectionManager, sasTokenManager);
                }
                else if (response.Type == "connection_details_result")
                {
                    connectionManager.HandleConnectionDetailsResponse(response.Payload);
                }
                else if (response.Type == "sas_token_result")
                {
                    sasTokenManager.HandleSasTokenResponse(response.Payload);
                }
            }
        });

        var sendTask = Task.Run(async () =>
        {
            var sequence = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                var message = new AgentMessage
                {
                    AgentId = agentId,
                    Type = "heartbeat",
                    Payload = $"seq={sequence++}",
                    Oid = oid,
                    Tid = tid
                };

                await call.RequestStream.WriteAsync(message, cts.Token);
                Console.WriteLine($"[Agent] Sent heartbeat seq={sequence - 1}");
                await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            }
        });

        await Task.WhenAny(receiveTask, sendTask);

        await call.RequestStream.CompleteAsync();
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

static async Task HandleValidateSqlAsync(
    AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
    string agentId, string oid, string tid, string payload)
{
    string correlationId = "";
    try
    {
        using var envelope = JsonDocument.Parse(payload);
        correlationId = envelope.RootElement.GetProperty("correlationId").GetString() ?? "";
        var dataJson = envelope.RootElement.GetProperty("data").GetString() ?? "{}";

        using var paramsDoc = JsonDocument.Parse(dataJson);
        var root = paramsDoc.RootElement;

        var serverName = root.GetProperty("serverName").GetString() ?? "";
        var databaseName = root.TryGetProperty("databaseName", out var dbEl) ? dbEl.GetString() ?? "" : "";
        var encrypt = root.TryGetProperty("encrypt", out var encEl) ? encEl.GetString() ?? "Mandatory" : "Mandatory";
        var trustCert = root.TryGetProperty("trustServerCertificate", out var tcEl) && tcEl.GetBoolean();
        var authentication = root.TryGetProperty("authentication", out var authEl)
            ? authEl.GetString() ?? "Microsoft Entra Integrated"
            : "Microsoft Entra Integrated";

        var csb = new SqlConnectionStringBuilder
        {
            DataSource = serverName,
            Encrypt = encrypt == "Mandatory" ? SqlConnectionEncryptOption.Mandatory
                    : encrypt == "Strict" ? SqlConnectionEncryptOption.Strict
                    : SqlConnectionEncryptOption.Optional,
            TrustServerCertificate = trustCert,
        };

        if (!string.IsNullOrEmpty(databaseName))
            csb.InitialCatalog = databaseName;

        if (authentication == "Microsoft Entra Integrated")
        {
            csb.Authentication = SqlAuthenticationMethod.ActiveDirectoryIntegrated;
        }
        else
        {
            var userName = root.TryGetProperty("userName", out var uEl) ? uEl.GetString() ?? "" : "";
            var password = root.TryGetProperty("password", out var pEl) ? pEl.GetString() ?? "" : "";
            csb.UserID = userName;
            csb.Password = password;
        }

        Console.WriteLine($"[Agent] Testing SQL connection to {serverName}...");

        await using var conn = new SqlConnection(csb.ConnectionString);
        await conn.OpenAsync();

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            message = $"Connection successful. Server version: {conn.ServerVersion}"
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "validate_sql_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });

        Console.WriteLine("[Agent] SQL connection test succeeded.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] SQL connection test failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Connection failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "validate_sql_result",
                Payload = resultPayload,
                Oid = oid,
                Tid = tid
            });
        }
        catch (Exception writeEx)
        {
            Console.Error.WriteLine($"[Agent] Failed to send error result: {writeEx.Message}");
        }
    }
}

static async Task HandleListTablesAsync(
    AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
    string agentId, string oid, string tid, string payload,
    SqlConnectionManager connManager)
{
    string correlationId = "";
    try
    {
        using var envelope = JsonDocument.Parse(payload);
        correlationId = envelope.RootElement.GetProperty("correlationId").GetString() ?? "";
        var dataJson = envelope.RootElement.GetProperty("data").GetString() ?? "{}";

        using var paramsDoc = JsonDocument.Parse(dataJson);
        var rowKey = paramsDoc.RootElement.GetProperty("rowKey").GetString() ?? "";

        Console.WriteLine($"[Agent] Listing tables for connection {rowKey}...");

        var tables = await connManager.ExecuteWithRetryAsync(rowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                var result = new List<object>();
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = @"SELECT TABLE_SCHEMA, TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    ORDER BY TABLE_SCHEMA, TABLE_NAME";

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    result.Add(new
                    {
                        schema = reader.GetString(0),
                        name = reader.GetString(1)
                    });
                }
                return result;
            });

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            tables
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "list_tables_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });

        Console.WriteLine($"[Agent] Listed {tables.Count} tables for connection {rowKey}.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] List tables failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"List tables failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "list_tables_result",
                Payload = resultPayload,
                Oid = oid,
                Tid = tid
            });
        }
        catch (Exception writeEx)
        {
            Console.Error.WriteLine($"[Agent] Failed to send error result: {writeEx.Message}");
        }
    }
}

static async Task HandlePreviewTableAsync(
    AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
    string agentId, string oid, string tid, string payload,
    SqlConnectionManager connManager, SasTokenManager sasManager)
{
    string correlationId = "";
    try
    {
        using var envelope = JsonDocument.Parse(payload);
        correlationId = envelope.RootElement.GetProperty("correlationId").GetString() ?? "";
        var dataJson = envelope.RootElement.GetProperty("data").GetString() ?? "{}";

        using var paramsDoc = JsonDocument.Parse(dataJson);
        var rowKey = paramsDoc.RootElement.GetProperty("rowKey").GetString() ?? "";
        var schema = paramsDoc.RootElement.GetProperty("schema").GetString() ?? "";
        var tableName = paramsDoc.RootElement.GetProperty("tableName").GetString() ?? "";

        Console.WriteLine($"[Agent] Previewing table [{schema}].[{tableName}] for connection {rowKey}...");

        var csvContent = await connManager.ExecuteWithRetryAsync(rowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM [{schema}].[{tableName}] TABLESAMPLE (200 ROWS)";

                await using var reader = await cmd.ExecuteReaderAsync();
                var sb = new StringBuilder();

                var columnCount = reader.FieldCount;
                for (int i = 0; i < columnCount; i++)
                {
                    if (i > 0) sb.Append(',');
                    sb.Append(CsvEnclose(reader.GetName(i)));
                }
                sb.AppendLine();

                while (await reader.ReadAsync())
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        if (i > 0) sb.Append(',');
                        var value = reader.IsDBNull(i) ? "" : reader.GetValue(i)?.ToString() ?? "";
                        sb.Append(CsvEnclose(value));
                    }
                    sb.AppendLine();
                }

                return sb.ToString();
            });

        var filename = $"{Guid.NewGuid():N}_preview.csv";

        var sasInfo = await sasManager.RequestSasTokenAsync(call, agentId, oid, tid);

        var blobUri = new Uri($"{sasInfo.BlobEndpoint}/{sasInfo.Container}/{filename}?{sasInfo.SasToken}");
        var blobClient = new BlobClient(blobUri);
        var csvBytes = Encoding.UTF8.GetBytes(csvContent);
        using var stream = new MemoryStream(csvBytes);
        await blobClient.UploadAsync(stream, overwrite: true);

        Console.WriteLine($"[Agent] Uploaded preview CSV: {filename}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            filename
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "preview_table_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] Preview table failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Preview failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "preview_table_result",
                Payload = resultPayload,
                Oid = oid,
                Tid = tid
            });
        }
        catch (Exception writeEx)
        {
            Console.Error.WriteLine($"[Agent] Failed to send error result: {writeEx.Message}");
        }
    }
}

static async Task HandleValidateQueryAsync(
    AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
    string agentId, string oid, string tid, string payload,
    SqlConnectionManager connManager)
{
    string correlationId = "";
    try
    {
        using var envelope = JsonDocument.Parse(payload);
        correlationId = envelope.RootElement.GetProperty("correlationId").GetString() ?? "";
        var dataJson = envelope.RootElement.GetProperty("data").GetString() ?? "{}";

        using var paramsDoc = JsonDocument.Parse(dataJson);
        var connectionRowKey = paramsDoc.RootElement.GetProperty("connectionRowKey").GetString() ?? "";
        var queryText = paramsDoc.RootElement.GetProperty("queryText").GetString() ?? "";

        Console.WriteLine($"[Agent] Validating query for connection {connectionRowKey}...");

        var message = await connManager.ExecuteWithRetryAsync(connectionRowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                await using var cmdOn = sqlConn.CreateCommand();
                cmdOn.CommandText = "SET NOEXEC ON";
                await cmdOn.ExecuteNonQueryAsync();

                try
                {
                    await using var cmdQuery = sqlConn.CreateCommand();
                    cmdQuery.CommandText = queryText;
                    await cmdQuery.ExecuteNonQueryAsync();
                    return "Query syntax is valid.";
                }
                finally
                {
                    await using var cmdOff = sqlConn.CreateCommand();
                    cmdOff.CommandText = "SET NOEXEC OFF";
                    await cmdOff.ExecuteNonQueryAsync();
                }
            });

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            message
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "validate_query_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });

        Console.WriteLine("[Agent] Query validation succeeded.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] Query validation failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Query validation failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "validate_query_result",
                Payload = resultPayload,
                Oid = oid,
                Tid = tid
            });
        }
        catch (Exception writeEx)
        {
            Console.Error.WriteLine($"[Agent] Failed to send error result: {writeEx.Message}");
        }
    }
}

static async Task HandlePreviewQueryAsync(
    AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
    string agentId, string oid, string tid, string payload,
    SqlConnectionManager connManager, SasTokenManager sasManager)
{
    string correlationId = "";
    try
    {
        using var envelope = JsonDocument.Parse(payload);
        correlationId = envelope.RootElement.GetProperty("correlationId").GetString() ?? "";
        var dataJson = envelope.RootElement.GetProperty("data").GetString() ?? "{}";

        using var paramsDoc = JsonDocument.Parse(dataJson);
        var connectionRowKey = paramsDoc.RootElement.GetProperty("connectionRowKey").GetString() ?? "";
        var queryText = paramsDoc.RootElement.GetProperty("queryText").GetString() ?? "";

        Console.WriteLine($"[Agent] Previewing query for connection {connectionRowKey}...");

        var csvContent = await connManager.ExecuteWithRetryAsync(connectionRowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = $"SELECT TOP 200 * FROM ({queryText}) AS _q";

                await using var reader = await cmd.ExecuteReaderAsync();
                var sb = new StringBuilder();

                var columnCount = reader.FieldCount;
                for (int i = 0; i < columnCount; i++)
                {
                    if (i > 0) sb.Append(',');
                    sb.Append(CsvEnclose(reader.GetName(i)));
                }
                sb.AppendLine();

                while (await reader.ReadAsync())
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        if (i > 0) sb.Append(',');
                        var value = reader.IsDBNull(i) ? "" : reader.GetValue(i)?.ToString() ?? "";
                        sb.Append(CsvEnclose(value));
                    }
                    sb.AppendLine();
                }

                return sb.ToString();
            });

        var filename = $"{Guid.NewGuid():N}_preview.csv";

        var sasInfo = await sasManager.RequestSasTokenAsync(call, agentId, oid, tid);

        var blobUri = new Uri($"{sasInfo.BlobEndpoint}/{sasInfo.Container}/{filename}?{sasInfo.SasToken}");
        var blobClient = new BlobClient(blobUri);
        var csvBytes = Encoding.UTF8.GetBytes(csvContent);
        using var stream = new MemoryStream(csvBytes);
        await blobClient.UploadAsync(stream, overwrite: true);

        Console.WriteLine($"[Agent] Uploaded query preview CSV: {filename}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            filename
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "preview_query_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] Preview query failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Preview query failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "preview_query_result",
                Payload = resultPayload,
                Oid = oid,
                Tid = tid
            });
        }
        catch (Exception writeEx)
        {
            Console.Error.WriteLine($"[Agent] Failed to send error result: {writeEx.Message}");
        }
    }
}

static string CsvEnclose(string value)
{
    return "\"" + value.Replace("\"", "\"\"") + "\"";
}

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
        // fallback below
    }

    var host = Dns.GetHostEntry(Dns.GetHostName());
    var ipv4 = host.AddressList.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork);
    return ipv4?.ToString() ?? "127.0.0.1";
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
}

class DataEngineConfig
{
    public string EngineUrl { get; set; } = "";
    public string AuthorizationToken { get; set; } = "";
    public string EnvironmentId { get; set; } = "";
    public string ConnectorId { get; set; } = "";
    public string ProfileSetId { get; set; } = "";
}

class SqlConnectionManager : IDisposable
{
    private readonly ConcurrentDictionary<string, ConnectionDetails> _details = new();
    private readonly ConcurrentDictionary<string, SqlConnection> _connections = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<ConnectionDetails>> _pendingDetailRequests = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public void LoadConnectionDetails(string connectionsListJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(connectionsListJson);
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                var details = ParseConnectionDetails(el);
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

    public void HandleConnectionDetailsResponse(string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var correlationId = doc.RootElement.TryGetProperty("correlationId", out var cidEl)
                ? cidEl.GetString() ?? "" : "";
            var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

            if (!string.IsNullOrEmpty(correlationId) &&
                _pendingDetailRequests.TryRemove(correlationId, out var tcs))
            {
                if (success)
                {
                    var details = ParseConnectionDetails(doc.RootElement);
                    _details[details.RowKey] = details;
                    tcs.TrySetResult(details);
                }
                else
                {
                    var message = doc.RootElement.TryGetProperty("message", out var msgEl)
                        ? msgEl.GetString() ?? "Unknown error" : "Unknown error";
                    tcs.TrySetException(new InvalidOperationException(message));
                }
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ConnMgr] Failed to handle connection details response: {ex.Message}");
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
        string rowKey,
        AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
        string agentId, string oid, string tid,
        Func<SqlConnection, Task<T>> operation)
    {
        try
        {
            var conn = await GetOrCreateConnectionAsync(rowKey);
            return await operation(conn);
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"[ConnMgr] SQL error for {rowKey}, attempting reconnect: {ex.Message}");

            EvictConnection(rowKey);

            try
            {
                await RefreshConnectionDetailsAsync(rowKey, call, agentId, oid, tid);
            }
            catch (Exception refetchEx)
            {
                Console.Error.WriteLine($"[ConnMgr] Failed to re-fetch details for {rowKey}: {refetchEx.Message}");
            }

            var conn = await GetOrCreateConnectionAsync(rowKey);
            return await operation(conn);
        }
    }

    private async Task RefreshConnectionDetailsAsync(
        string rowKey,
        AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
        string agentId, string oid, string tid)
    {
        var correlationId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<ConnectionDetails>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingDetailRequests[correlationId] = tcs;

        try
        {
            var requestPayload = JsonSerializer.Serialize(new { correlationId, rowKey });
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "get_connection_details",
                Payload = requestPayload,
                Oid = oid,
                Tid = tid
            });

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await using var reg = timeout.Token.Register(() =>
                tcs.TrySetException(new TimeoutException("Timed out waiting for connection details")));

            await tcs.Task;
            Console.WriteLine($"[ConnMgr] Refreshed connection details for {rowKey}");
        }
        finally
        {
            _pendingDetailRequests.TryRemove(correlationId, out _);
        }
    }

    private void EvictConnection(string rowKey)
    {
        if (_connections.TryRemove(rowKey, out var old))
        {
            try { old.Dispose(); } catch { }
        }
    }

    private static SqlConnection BuildSqlConnection(ConnectionDetails details)
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

    private static ConnectionDetails ParseConnectionDetails(JsonElement el)
    {
        return new ConnectionDetails
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

    public void Dispose()
    {
        foreach (var kvp in _connections)
        {
            try { kvp.Value.Dispose(); } catch { }
        }
        _connections.Clear();
        _lock.Dispose();
    }
}

class SasTokenInfo
{
    public string SasToken { get; set; } = "";
    public string BlobEndpoint { get; set; } = "";
    public string Container { get; set; } = "";
}

class SasTokenManager
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<SasTokenInfo>> _pending = new();

    public async Task<SasTokenInfo> RequestSasTokenAsync(
        AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
        string agentId, string oid, string tid)
    {
        var correlationId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<SasTokenInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[correlationId] = tcs;

        try
        {
            var requestPayload = JsonSerializer.Serialize(new { correlationId });
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "request_sas_token",
                Payload = requestPayload,
                Oid = oid,
                Tid = tid
            });

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await using var reg = timeout.Token.Register(() =>
                tcs.TrySetException(new TimeoutException("Timed out waiting for SAS token")));

            return await tcs.Task;
        }
        finally
        {
            _pending.TryRemove(correlationId, out _);
        }
    }

    public void HandleSasTokenResponse(string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var correlationId = doc.RootElement.TryGetProperty("correlationId", out var cidEl)
                ? cidEl.GetString() ?? "" : "";

            if (!string.IsNullOrEmpty(correlationId) && _pending.TryRemove(correlationId, out var tcs))
            {
                var info = new SasTokenInfo
                {
                    SasToken = doc.RootElement.TryGetProperty("sasToken", out var st) ? st.GetString() ?? "" : "",
                    BlobEndpoint = doc.RootElement.TryGetProperty("blobEndpoint", out var be) ? be.GetString() ?? "" : "",
                    Container = doc.RootElement.TryGetProperty("container", out var ct) ? ct.GetString() ?? "" : ""
                };
                tcs.TrySetResult(info);
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[SasTokenMgr] Failed to handle SAS token response: {ex.Message}");
        }
    }
}
