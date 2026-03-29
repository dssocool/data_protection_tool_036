using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Azure.Identity;
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

        if (await call.ResponseStream.MoveNext(cts.Token))
        {
            var response = call.ResponseStream.Current;
            if (response.Type == "registration_url")
            {
                Console.WriteLine($"Agent registered. URL: {response.Payload}");
            }
            else
            {
                Console.WriteLine($"[Server] type={response.Type}, payload={response.Payload}");
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
