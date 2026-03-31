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
using Parquet;
using Parquet.Data;
using Parquet.Schema;
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

        var receiveTask = Task.Run(async () =>
        {
            while (await call.ResponseStream.MoveNext(cts.Token))
            {
                var response = call.ResponseStream.Current;

                if (response.Type == "registration_url")
                {
                    Console.WriteLine($"Agent registered. URL: {response.Payload}");
                    continue;
                }

                if (response.Type == "connections_list")
                {
                    connectionManager.LoadConnectionDetails(response.Payload);
                    Console.WriteLine("[Agent] Loaded connection details from ControlCenter.");
                    continue;
                }

                if (response.Type == "ack")
                {
                    continue;
                }

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
                else if (response.Type == "http_request")
                {
                    _ = HandleHttpRequestAsync(call, agentId, oid, tid, response.Payload);
                }
                else if (response.Type == "fetch_sql_types")
                {
                    _ = HandleFetchSqlTypesAsync(call, agentId, oid, tid, response.Payload, connectionManager);
                }
                else if (response.Type == "list_schemas")
                {
                    _ = HandleListSchemasAsync(call, agentId, oid, tid, response.Payload, connectionManager);
                }
                else if (response.Type == "export_table")
                {
                    _ = HandleExportTableAsync(call, agentId, oid, tid, response.Payload, connectionManager, sasTokenManager);
                }
                else if (response.Type == "load_masked_to_table")
                {
                    _ = HandleLoadMaskedToTableAsync(call, agentId, oid, tid, response.Payload, connectionManager, sasTokenManager);
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
                await Task.Delay(TimeSpan.FromSeconds(60), cts.Token);
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

static async Task HandleFetchSqlTypesAsync(
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
        var schema = paramsDoc.RootElement.GetProperty("schema").GetString() ?? "";
        var tableName = paramsDoc.RootElement.GetProperty("tableName").GetString() ?? "";

        Console.WriteLine($"[Agent] Fetching SQL types for [{schema}].[{tableName}] on connection {rowKey}...");

        var columns = await connManager.ExecuteWithRetryAsync(rowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                var result = new List<object>();
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = @"SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName
                    ORDER BY ORDINAL_POSITION";
                cmd.Parameters.AddWithValue("@schema", schema);
                cmd.Parameters.AddWithValue("@tableName", tableName);

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    result.Add(new
                    {
                        name = reader.GetString(0),
                        dataType = reader.GetString(1)
                    });
                }
                return result;
            });

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            columns
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "fetch_sql_types_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });

        Console.WriteLine($"[Agent] Fetched SQL types for {columns.Count} columns in [{schema}].[{tableName}].");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] Fetch SQL types failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Fetch SQL types failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "fetch_sql_types_result",
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

static async Task HandleListSchemasAsync(
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

        Console.WriteLine($"[Agent] Listing schemas for connection {rowKey}...");

        var schemas = await connManager.ExecuteWithRetryAsync(rowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                var result = new List<string>();
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = @"SELECT DISTINCT TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA";

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    result.Add(reader.GetString(0));
                }
                return result;
            });

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            schemas
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "list_schemas_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });

        Console.WriteLine($"[Agent] Listed {schemas.Count} schemas for connection {rowKey}.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] List schemas failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"List schemas failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "list_schemas_result",
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

static async Task HandleExportTableAsync(
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
        var uniqueId = paramsDoc.RootElement.GetProperty("uniqueId").GetString() ?? "";

        if (string.IsNullOrWhiteSpace(uniqueId))
            throw new InvalidOperationException("Missing uniqueId in export request.");

        Console.WriteLine($"[Agent] Exporting full table [{schema}].[{tableName}] for connection {rowKey}...");

        var filenames = await connManager.ExecuteWithRetryAsync(rowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM [{schema}].[{tableName}]";
                cmd.CommandTimeout = 0;

                await using var reader = await cmd.ExecuteReaderAsync();
                return await StreamReaderToParquetBlobs(reader, call, agentId, oid, tid, uniqueId, sasManager);
            });

        Console.WriteLine($"[Agent] Exported {filenames.Count} Parquet file(s) for [{schema}].[{tableName}]");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            filenames
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "export_table_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] Export table failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Export failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "export_table_result",
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

static async Task HandleLoadMaskedToTableAsync(
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
        var destRowKey = paramsDoc.RootElement.GetProperty("destRowKey").GetString() ?? "";
        var destSchema = paramsDoc.RootElement.GetProperty("destSchema").GetString() ?? "";
        var tableName = paramsDoc.RootElement.GetProperty("tableName").GetString() ?? "";
        var blobFilename = paramsDoc.RootElement.GetProperty("blobFilename").GetString() ?? "";
        var createTable = paramsDoc.RootElement.TryGetProperty("createTable", out var ctEl) && ctEl.GetBoolean();
        var truncate = paramsDoc.RootElement.TryGetProperty("truncate", out var trEl) && trEl.GetBoolean();

        Console.WriteLine($"[Agent] Loading masked file {blobFilename} -> [{destSchema}].[{tableName}] (create={createTable}, truncate={truncate})");

        var sasInfo = await sasManager.RequestSasTokenAsync(call, agentId, oid, tid);
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
                sqlTypes = JsonSerializer.Deserialize<string[]>(sqlTypesJson) ?? columnNames.Select(_ => "nvarchar(max)").ToArray();
            }
            else
            {
                sqlTypes = columnNames.Select(_ => "nvarchar(max)").ToArray();
            }

            await connManager.ExecuteWithRetryAsync(destRowKey, call, agentId, oid, tid,
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

            var resultPayload = JsonSerializer.Serialize(new
            {
                correlationId,
                success = true
            });

            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "load_masked_to_table_result",
                Payload = resultPayload,
                Oid = oid,
                Tid = tid
            });
        }
        finally
        {
            try { File.Delete(tempFile); } catch { }
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] Load masked to table failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"Load masked to table failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "load_masked_to_table_result",
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
        var uniqueId = paramsDoc.RootElement.GetProperty("uniqueId").GetString() ?? "";

        if (string.IsNullOrWhiteSpace(uniqueId))
            throw new InvalidOperationException("Missing uniqueId in preview request.");

        Console.WriteLine($"[Agent] Previewing table [{schema}].[{tableName}] for connection {rowKey}...");

        var filenames = await connManager.ExecuteWithRetryAsync(rowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM [{schema}].[{tableName}] TABLESAMPLE (200 ROWS)";

                await using var reader = await cmd.ExecuteReaderAsync();
                return await StreamReaderToParquetBlobs(reader, call, agentId, oid, tid, uniqueId, sasManager);
            });

        Console.WriteLine($"[Agent] Uploaded {filenames.Count} preview Parquet file(s) for [{schema}].[{tableName}]");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            filenames
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
        var uniqueId = paramsDoc.RootElement.GetProperty("uniqueId").GetString() ?? "";

        if (string.IsNullOrWhiteSpace(uniqueId))
            throw new InvalidOperationException("Missing uniqueId in preview request.");

        Console.WriteLine($"[Agent] Previewing query for connection {connectionRowKey}...");

        var filenames = await connManager.ExecuteWithRetryAsync(connectionRowKey, call, agentId, oid, tid,
            async (sqlConn) =>
            {
                await using var cmd = sqlConn.CreateCommand();
                cmd.CommandText = $"SELECT TOP 200 * FROM ({queryText}) AS _q";

                await using var reader = await cmd.ExecuteReaderAsync();
                return await StreamReaderToParquetBlobs(reader, call, agentId, oid, tid, uniqueId, sasManager);
            });

        Console.WriteLine($"[Agent] Uploaded {filenames.Count} query preview Parquet file(s)");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            filenames
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

static async Task HandleHttpRequestAsync(
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
        {
            requestMessage.Content = new StringContent(bodyContent, Encoding.UTF8);
        }

        using var response = await httpClient.SendAsync(requestMessage);
        var responseBody = await response.Content.ReadAsStringAsync();

        var responseHeaders = new Dictionary<string, string>();
        foreach (var h in response.Headers)
            responseHeaders[h.Key] = string.Join(", ", h.Value);
        foreach (var h in response.Content.Headers)
            responseHeaders[h.Key] = string.Join(", ", h.Value);

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = true,
            statusCode = (int)response.StatusCode,
            headers = responseHeaders,
            body = responseBody
        });

        await call.RequestStream.WriteAsync(new AgentMessage
        {
            AgentId = agentId,
            Type = "http_request_result",
            Payload = resultPayload,
            Oid = oid,
            Tid = tid
        });

        Console.WriteLine($"[Agent] HTTP relay completed: {(int)response.StatusCode} {response.StatusCode}");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Agent] HTTP relay failed: {ex.Message}");

        var resultPayload = JsonSerializer.Serialize(new
        {
            correlationId,
            success = false,
            message = $"HTTP request failed: {ex.Message}"
        });

        try
        {
            await call.RequestStream.WriteAsync(new AgentMessage
            {
                AgentId = agentId,
                Type = "http_request_result",
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

const int PreviewBatchSize = 10_000;

static async Task<List<string>> StreamReaderToParquetBlobs(
    SqlDataReader reader,
    AsyncDuplexStreamingCall<AgentMessage, ServerMessage> call,
    string agentId, string oid, string tid, string uniqueId,
    SasTokenManager sasManager)
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
        ["sql_types"] = JsonSerializer.Serialize(sqlTypes)
    };

    var parquetSchema = new ParquetSchema(dataFields);
    var sasInfo = await sasManager.RequestSasTokenAsync(call, agentId, oid, tid);
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

        var filename = BuildPreviewFilename(uniqueId, previewRequestUuid, fileSequence);
        var blobUri = new Uri($"{sasInfo.BlobEndpoint}/{sasInfo.Container}/{filename}?{sasInfo.SasToken}");
        var blobClient = new BlobClient(blobUri);
        ms.Position = 0;
        await blobClient.UploadAsync(ms, overwrite: true);
        filenames.Add(filename);
        fileSequence++;

        Console.WriteLine($"[Agent] Uploaded batch Parquet ({rowsInBatch} rows): {filename}");
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

        var filename = BuildPreviewFilename(uniqueId, previewRequestUuid, fileSequence);
        var blobUri = new Uri($"{sasInfo.BlobEndpoint}/{sasInfo.Container}/{filename}?{sasInfo.SasToken}");
        var blobClient = new BlobClient(blobUri);
        ms.Position = 0;
        await blobClient.UploadAsync(ms, overwrite: true);
        filenames.Add(filename);

        Console.WriteLine($"[Agent] Uploaded empty preview Parquet: {filename}");
    }

    return filenames;
}

static string BuildPreviewFilename(string uniqueId, string previewUuid, int fileSequence)
{
    if (fileSequence <= 1)
        return $"preview_{uniqueId}_{previewUuid}.parquet";

    return $"preview_{uniqueId}_{previewUuid}_{fileSequence}.parquet";
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
        catch (Exception ex) when (ex is SqlException || ex is InvalidOperationException)
        {
            Console.WriteLine($"[ConnMgr] Error for {rowKey}, attempting reconnect: {ex.Message}");

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
