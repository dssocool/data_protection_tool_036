using System.Text.Json;
using DataProtectionTool.ControlCenter.Helpers;
using DataProtectionTool.ControlCenter.Models;
using DataProtectionTool.ControlCenter.Services;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.Endpoints;

public static class AgentEndpoints
{
    public static void MapAgentEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/agents/{path}", (string path, AgentRegistry registry) =>
        {
            if (!registry.TryGet(path, out var info) || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            return Results.Ok(new
            {
                oid = info.Oid,
                tid = info.Tid,
                agentId = info.AgentId,
                connectedAt = info.ConnectedAt.ToString("O"),
                userName = info.UserName
            });
        });

        app.MapGet("/api/agents/{path}/user-id", async (string path, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGet(path, out var info) || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
            return Results.Ok(new { uniqueId });
        });

        app.MapPost("/api/agents/{path}/validate-sql", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            try
            {
                var result = await connection.SendCommandAsync("validate_sql", body, TimeSpan.FromSeconds(30));

                using var doc = JsonDocument.Parse(result);
                var message = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;
                var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

                var evtSummary = success ? "SQL validation: success" : $"SQL validation: failed — {message}";
                var evtDetail = message ?? "";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", evtSummary, evtDetail);

                return EndpointHelpers.EventResult(success, message ?? "Unknown result", "validate_sql", evtSummary, evtDetail);
            }
            catch (TimeoutException)
            {
                var evtSummary = "SQL validation: timeout";
                var evtDetail = "Agent did not respond within 30 seconds.";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", evtSummary, evtDetail);
                return EndpointHelpers.EventResult(false, "Agent did not respond within 30 seconds.", "validate_sql", evtSummary, evtDetail);
            }
            catch (Exception ex)
            {
                var evtSummary = $"SQL validation: error — {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", evtSummary);
                return EndpointHelpers.EventResult(false, $"Validation error: {ex.Message}", "validate_sql", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/save-connection", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGet(path, out var info) || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var entity = await clientTableService.SaveConnectionAsync(
                    partitionKey,
                    root.TryGetProperty("serverName", out var sn) ? sn.GetString() ?? "" : "",
                    root.TryGetProperty("authentication", out var au) ? au.GetString() ?? "" : "",
                    root.TryGetProperty("userName", out var un) ? un.GetString() ?? "" : "",
                    root.TryGetProperty("password", out var pw) ? pw.GetString() ?? "" : "",
                    root.TryGetProperty("databaseName", out var db) ? db.GetString() ?? "" : "",
                    root.TryGetProperty("encrypt", out var en) ? en.GetString() ?? "" : "",
                    root.TryGetProperty("trustServerCertificate", out var tsc) && tsc.GetBoolean());

                if (registry.TryGetConnection(path, out var connection) && connection is not null)
                {
                    try
                    {
                        var connections = await clientTableService.GetConnectionsAsync(partitionKey);
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
                        await connection.ResponseStream.WriteAsync(new ServerMessage
                        {
                            Type = "connections_list",
                            Payload = connectionsJson
                        });
                    }
                    catch
                    {
                    }
                }

                var serverName = root.TryGetProperty("serverName", out var snEvt) ? snEvt.GetString() ?? "" : "";
                var evtSummary = $"Connection saved: {serverName}";
                _ = clientTableService.AppendEventAsync(partitionKey, "save_connection", evtSummary);

                return EndpointHelpers.EventResultWithRowKey(true, "Connection saved.", entity.RowKey, "save_connection", evtSummary);
            }
            catch (Exception ex)
            {
                var pk = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var evtSummary = $"Save connection failed: {ex.Message}";
                _ = clientTableService.AppendEventAsync(pk, "save_connection", evtSummary);
                return EndpointHelpers.EventResult(false, $"Failed to save: {ex.Message}", "save_connection", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/list-tables", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService, ILogger<AgentRegistry> logger) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            string rowKey;
            bool refresh = false;
            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                rowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
                refresh = bodyDoc.RootElement.TryGetProperty("refresh", out var rfEl) && rfEl.GetBoolean();
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid request body." });
            }

            var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
            if (connEntity == null)
                return Results.NotFound(new { error = "Connection not found." });

            if (!refresh)
            {
                var cached = await clientTableService.GetDataItemsAsync(partitionKey, connEntity.ServerName, connEntity.DatabaseName);
                if (cached.Count > 0)
                {
                    var cachedTables = cached.Select(d => new { schema = d.Schema, name = d.TableName, fileFormatId = d.FileFormatId }).ToList();
                    var evtSummary = $"Listed {cachedTables.Count} tables (cached)";
                    _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                    return Results.Ok(new
                    {
                        success = true,
                        tables = cachedTables,
                        @event = EndpointHelpers.EventPayload("list_tables", evtSummary)
                    });
                }
            }

            try
            {
                var listTablesPayload = JsonSerializer.Serialize(new
                {
                    rowKey,
                    sqlStatement = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME"
                });
                var result = await connection.SendCommandAsync("execute_sql", listTablesPayload, TimeSpan.FromSeconds(30));

                try
                {
                    using var doc = JsonDocument.Parse(result);
                    if (doc.RootElement.TryGetProperty("rows", out var tEl) && tEl.ValueKind == JsonValueKind.Array)
                    {
                        var tableList = new List<(string schema, string name)>();
                        foreach (var item in tEl.EnumerateArray())
                        {
                            var schema = item.TryGetProperty("TABLE_SCHEMA", out var sEl) ? sEl.GetString() ?? "" : "";
                            var name = item.TryGetProperty("TABLE_NAME", out var nEl) ? nEl.GetString() ?? "" : "";
                            tableList.Add((schema, name));
                        }
                        var tables = tableList.Select(t => new { schema = t.schema, name = t.name }).ToList();
                        var evtSummary = $"Listed {tableList.Count} tables";

                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                await clientTableService.SaveDataItemsAsync(partitionKey, connEntity.ServerName, connEntity.DatabaseName, rowKey, tableList);
                                await clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                            }
                            catch (Exception saveEx)
                            {
                                await clientTableService.AppendEventAsync(partitionKey, "list_tables",
                                    $"Failed to load tables from {connEntity.DatabaseName}. Refresh the database to try again.");
                                await clientTableService.AppendEventAsync(partitionKey, "list_tables",
                                    saveEx.Message, saveEx.ToString());
                            }
                        });

                        return Results.Ok(new
                        {
                            success = true,
                            tables,
                            @event = EndpointHelpers.EventPayload("list_tables", evtSummary)
                        });
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to parse/save tables from agent response");
                }

                return Results.Content(result, "application/json");
            }
            catch (TimeoutException)
            {
                var evtSummary = "List tables: timeout";
                _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                return EndpointHelpers.EventResult(false, "Agent did not respond within 30 seconds.", "list_tables", evtSummary);
            }
            catch (Exception ex)
            {
                var evtSummary = $"List tables error: {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                return EndpointHelpers.EventResult(false, $"List tables error: {ex.Message}", "list_tables", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/list-columns", async (string path, HttpRequest request, AgentRegistry registry) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            var body = await request.ReadBodyAsync();

            string rowKey;
            string schema;
            string tableName;
            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                rowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
                schema = bodyDoc.RootElement.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
                tableName = bodyDoc.RootElement.TryGetProperty("tableName", out var tnEl) ? tnEl.GetString() ?? "" : "";
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid request body." });
            }

            if (string.IsNullOrEmpty(rowKey) || string.IsNullOrEmpty(schema) || string.IsNullOrEmpty(tableName))
                return Results.BadRequest(new { error = "rowKey, schema and tableName are required." });

            try
            {
                var payload = JsonSerializer.Serialize(new
                {
                    rowKey,
                    sqlStatement = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION",
                    sqlParams = new Dictionary<string, string> { ["@schema"] = schema, ["@tableName"] = tableName }
                });
                var result = await connection.SendCommandAsync("execute_sql", payload, TimeSpan.FromSeconds(30));

                using var doc = JsonDocument.Parse(result);
                if (doc.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean()
                    && doc.RootElement.TryGetProperty("rows", out var rowsEl) && rowsEl.ValueKind == JsonValueKind.Array)
                {
                    var columns = new List<object>();
                    foreach (var item in rowsEl.EnumerateArray())
                    {
                        var colName = item.TryGetProperty("COLUMN_NAME", out var cnEl) ? cnEl.GetString() ?? "" : "";
                        var colType = item.TryGetProperty("DATA_TYPE", out var ctEl) ? ctEl.GetString() ?? "" : "";
                        columns.Add(new { name = colName, type = colType });
                    }
                    return Results.Ok(new { success = true, columns });
                }

                return Results.Content(result, "application/json");
            }
            catch (TimeoutException)
            {
                return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"List columns error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/list-schemas", async (string path, string rowKey, AgentRegistry registry) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            if (string.IsNullOrEmpty(rowKey))
                return Results.BadRequest(new { error = "rowKey query parameter is required." });

            try
            {
                var payload = JsonSerializer.Serialize(new
                {
                    rowKey,
                    sqlStatement = "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA"
                });
                var result = await connection.SendCommandAsync("execute_sql", payload, TimeSpan.FromSeconds(30));

                using var doc = JsonDocument.Parse(result);
                if (doc.RootElement.TryGetProperty("rows", out var rowsEl) && rowsEl.ValueKind == JsonValueKind.Array)
                {
                    var schemas = new List<string>();
                    foreach (var item in rowsEl.EnumerateArray())
                    {
                        if (item.TryGetProperty("TABLE_SCHEMA", out var sEl))
                            schemas.Add(sEl.GetString() ?? "");
                    }
                    return Results.Ok(new { success = true, schemas });
                }

                return Results.Content(result, "application/json");
            }
            catch (TimeoutException)
            {
                return Results.Ok(new { success = false, message = "Agent did not respond within 30 seconds." });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"List schemas error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/connections", async (string path, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGet(path, out var info) || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var connections = await clientTableService.GetConnectionsAsync(partitionKey);

            var result = connections.Select(c => new
            {
                rowKey = c.RowKey,
                connectionType = c.ConnectionType,
                serverName = c.ServerName,
                authentication = c.Authentication,
                databaseName = c.DatabaseName,
                encrypt = c.Encrypt,
                trustServerCertificate = c.TrustServerCertificate,
                createdAt = c.CreatedAt.ToString("O")
            });

            return Results.Ok(result);
        });

        app.MapPost("/api/agents/{path}/http-request", async (string path, HttpRequest request, AgentRegistry registry) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            var body = await request.ReadBodyAsync();

            try
            {
                var result = await connection.SendCommandAsync("http_request", body, TimeSpan.FromSeconds(120));
                return Results.Content(result, "application/json");
            }
            catch (TimeoutException)
            {
                return Results.Ok(new { success = false, message = "Agent did not respond within 120 seconds." });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"HTTP request relay error: {ex.Message}" });
            }
        });
    }
}
