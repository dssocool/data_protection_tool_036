using System.Text.Json;
using Azure.Storage;
using Azure.Storage.Sas;
using Grpc.Core;
using DataProtectionTool.Contracts;
using DataProtectionTool.ControlCenter.Models;

namespace DataProtectionTool.ControlCenter.Services;

public class AgentHubService : AgentHub.AgentHubBase
{
    private readonly ILogger<AgentHubService> _logger;
    private readonly AgentRegistry _registry;
    private readonly ClientTableService? _clientTableService;
    private readonly BlobStorageConfig _blobStorageConfig;
    private readonly StorageSharedKeyCredential? _blobCredential;
    private readonly CenterHealthStatus _healthStatus;

    public AgentHubService(
        ILogger<AgentHubService> logger,
        AgentRegistry registry,
        BlobStorageConfig blobStorageConfig,
        CenterHealthStatus healthStatus,
        ClientTableService? clientTableService = null,
        StorageSharedKeyCredential? blobCredential = null)
    {
        _logger = logger;
        _registry = registry;
        _clientTableService = clientTableService;
        _blobStorageConfig = blobStorageConfig;
        _blobCredential = blobCredential;
        _healthStatus = healthStatus;
    }

    public override async Task Connect(
        IAsyncStreamReader<AgentMessage> requestStream,
        IServerStreamWriter<ServerMessage> responseStream,
        ServerCallContext context)
    {
        var peer = context.Peer;
        _logger.LogInformation("Agent connected from {Peer}", peer);

        if (!_healthStatus.IsHealthy)
        {
            _logger.LogWarning("Rejecting agent from {Peer} — center has configuration issues", peer);
            await responseStream.WriteAsync(new ServerMessage
            {
                Type = "error",
                Payload = "Error: Center is having issues to start"
            });
            return;
        }

        string? registeredPath = null;

        try
        {
            if (!await requestStream.MoveNext(context.CancellationToken))
            {
                _logger.LogWarning("Agent from {Peer} disconnected without sending any messages", peer);
                return;
            }

            var firstMessage = requestStream.Current;

            var oid = firstMessage.Oid;
            var tid = firstMessage.Tid;
            var userName = firstMessage.UserName;

            if (string.IsNullOrEmpty(oid))
                oid = context.RequestHeaders.GetValue(SharedSecret.OidMetadataKey) ?? "";
            if (string.IsNullOrEmpty(tid))
                tid = context.RequestHeaders.GetValue(SharedSecret.TidMetadataKey) ?? "";
            if (string.IsNullOrEmpty(userName))
                userName = context.RequestHeaders.GetValue(SharedSecret.UserNameMetadataKey) ?? "";

            var agentInfo = new AgentInfo(oid, tid, firstMessage.AgentId, DateTime.UtcNow, userName);
            registeredPath = _registry.Register(agentInfo, responseStream);

            if (_clientTableService != null)
            {
                await _clientTableService.CreateOrUpdateClientAsync(oid, tid, firstMessage.AgentId, userName);

                var partitionKeyForEvents = Models.ClientEntity.BuildPartitionKey(oid, tid);
                _ = _clientTableService.AppendEventAsync(partitionKeyForEvents, "agent_connected",
                    $"Agent {firstMessage.AgentId} connected from {peer}");
            }

            var url = $"http://localhost:8190/agents/{registeredPath}";
            _logger.LogInformation(
                "Agent {AgentId} registered — oid={Oid}, tid={Tid}, url={Url}",
                firstMessage.AgentId, oid, tid, url);

            await responseStream.WriteAsync(new ServerMessage
            {
                Type = "registration_url",
                Payload = url
            });

            var partitionKey = Models.ClientEntity.BuildPartitionKey(oid, tid);
            try
            {
                var connections = _clientTableService != null
                    ? await _clientTableService.GetConnectionsAsync(partitionKey)
                    : new List<Models.ConnectionEntity>();
                var connectionsJson = JsonSerializer.Serialize(connections.Select(c => new
                {
                    rowKey = c.RowKey,
                    connectionType = c.ConnectionType,
                    serverName = c.ServerName,
                    authentication = c.Authentication,
                    userName = c.UserName,
                    password = c.Password,
                    databaseName = c.DatabaseName,
                    encrypt = c.Encrypt,
                    trustServerCertificate = c.TrustServerCertificate,
                }));
                await responseStream.WriteAsync(new ServerMessage
                {
                    Type = "connections_list",
                    Payload = connectionsJson
                });
                _logger.LogInformation("Pushed {Count} connections to agent {AgentId}", connections.Count, firstMessage.AgentId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to push connections to agent {AgentId}", firstMessage.AgentId);
            }

            var readTask = Task.Run(async () =>
            {
                while (await requestStream.MoveNext(context.CancellationToken))
                {
                    var message = requestStream.Current;
                    _logger.LogInformation(
                        "Received from agent {AgentId}: type={Type}, payload={Payload}",
                        message.AgentId, message.Type, message.Payload);

                    if (message.Type == "heartbeat")
                    {
                        await responseStream.WriteAsync(new ServerMessage
                        {
                            Type = "ack",
                            Payload = $"Received heartbeat from {message.AgentId}"
                        });
                        continue;
                    }

                    if (TryRouteCommandResponse(registeredPath!, message.Payload))
                        continue;

                    if (message.Type == "get_connection_details" && registeredPath != null)
                    {
                        _ = HandleGetConnectionDetailsAsync(responseStream, partitionKey, message.Payload);
                        continue;
                    }

                    if (message.Type == "request_sas_token")
                    {
                        _ = HandleSasTokenRequestAsync(responseStream, message.Payload);
                        continue;
                    }

                    await responseStream.WriteAsync(new ServerMessage
                    {
                        Type = "ack",
                        Payload = $"Received {message.Type} from {message.AgentId}"
                    });
                }
            });

            await readTask;
        }
        catch (OperationCanceledException)
        {
            // Client disconnected
        }
        finally
        {
            if (registeredPath != null)
            {
                if (_clientTableService != null && _registry.TryGetConnection(registeredPath, out var disc) && disc != null)
                {
                    var pk = Models.ClientEntity.BuildPartitionKey(disc.Info.Oid, disc.Info.Tid);
                    _ = _clientTableService.AppendEventAsync(pk, "agent_disconnected", "Agent disconnected");
                }
                _registry.Remove(registeredPath);
            }
        }

        _logger.LogInformation("Agent disconnected from {Peer}", peer);
    }

    private bool TryRouteCommandResponse(string agentPath, string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            if (!doc.RootElement.TryGetProperty("correlationId", out var cidEl))
                return false;

            var correlationId = cidEl.GetString();
            if (string.IsNullOrEmpty(correlationId))
                return false;

            if (_registry.TryGetConnection(agentPath, out var conn) && conn != null)
            {
                return conn.TryCompleteCommand(correlationId, payload);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to route command response from agent at {Path}", agentPath);
        }
        return false;
    }

    private async Task HandleSasTokenRequestAsync(
        IServerStreamWriter<ServerMessage> responseStream,
        string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var correlationId = doc.RootElement.TryGetProperty("correlationId", out var cidEl)
                ? cidEl.GetString() ?? "" : "";
            var requestedContainer = doc.RootElement.TryGetProperty("containerName", out var cnEl)
                ? cnEl.GetString() : null;

            var sasBuilder = new AccountSasBuilder
            {
                Services = AccountSasServices.Blobs,
                ResourceTypes = AccountSasResourceTypes.Object,
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15),
                Protocol = SasProtocol.HttpsAndHttp
            };
            sasBuilder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write | AccountSasPermissions.Create);

            if (_blobCredential == null)
                throw new InvalidOperationException("Blob storage credentials are not configured.");

            var sasToken = sasBuilder.ToSasQueryParameters(_blobCredential).ToString();

            var blobEndpoint = _blobStorageConfig.StorageAccount == "devstoreaccount1"
                ? $"http://127.0.0.1:10000/{_blobStorageConfig.StorageAccount}"
                : $"https://{_blobStorageConfig.StorageAccount}.blob.core.windows.net";

            var resultJson = JsonSerializer.Serialize(new
            {
                correlationId,
                sasToken,
                blobEndpoint,
                container = requestedContainer ?? _blobStorageConfig.PreviewContainer
            });

            await responseStream.WriteAsync(new ServerMessage
            {
                Type = "sas_token_result",
                Payload = resultJson
            });

            _logger.LogInformation("Generated SAS token for agent (correlationId={CorrelationId})", correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to handle SAS token request");
        }
    }

    private async Task HandleGetConnectionDetailsAsync(
        IServerStreamWriter<ServerMessage> responseStream,
        string partitionKey,
        string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var rowKey = doc.RootElement.GetProperty("rowKey").GetString() ?? "";
            var correlationId = doc.RootElement.TryGetProperty("correlationId", out var cidEl)
                ? cidEl.GetString() ?? "" : "";

            if (_clientTableService == null)
                throw new InvalidOperationException("Table service is not configured.");

            var entity = await _clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);

            string resultJson;
            if (entity != null)
            {
                resultJson = JsonSerializer.Serialize(new
                {
                    correlationId,
                    success = true,
                    rowKey = entity.RowKey,
                    serverName = entity.ServerName,
                    authentication = entity.Authentication,
                    userName = entity.UserName,
                    password = entity.Password,
                    databaseName = entity.DatabaseName,
                    encrypt = entity.Encrypt,
                    trustServerCertificate = entity.TrustServerCertificate,
                });
            }
            else
            {
                resultJson = JsonSerializer.Serialize(new
                {
                    correlationId,
                    success = false,
                    message = $"Connection {rowKey} not found."
                });
            }

            await responseStream.WriteAsync(new ServerMessage
            {
                Type = "connection_details_result",
                Payload = resultJson
            });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to handle get_connection_details");
        }
    }
}
