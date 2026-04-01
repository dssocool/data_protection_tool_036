using System.Text.Json;
using DataProtectionTool.ControlCenter.Helpers;
using DataProtectionTool.ControlCenter.Models;
using DataProtectionTool.ControlCenter.Services;

namespace DataProtectionTool.ControlCenter.Endpoints;

public static class QueryEndpoints
{
    public static void MapQueryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/validate-query", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            try
            {
                var validateQueryPayload = EndpointHelpers.AddFieldsToPayload(body, new
                {
                    sqlStatementBefore = "SET NOEXEC ON",
                    sqlStatementAfter = "SET NOEXEC OFF"
                });
                var result = await connection.SendCommandAsync("validate_query", validateQueryPayload, TimeSpan.FromSeconds(30));

                using var doc = JsonDocument.Parse(result);
                var message = doc.RootElement.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;
                var success = doc.RootElement.TryGetProperty("success", out var sEl) && sEl.GetBoolean();

                var evtSummary = success ? "Query validation: success" : $"Query validation: failed — {message}";
                var evtDetail = message ?? "";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", evtSummary, evtDetail);

                return EndpointHelpers.EventResult(success, message ?? "Unknown result", "validate_query", evtSummary, evtDetail);
            }
            catch (TimeoutException)
            {
                var evtSummary = "Query validation: timeout";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", evtSummary);
                return EndpointHelpers.EventResult(false, "Agent did not respond within 30 seconds.", "validate_query", evtSummary);
            }
            catch (Exception ex)
            {
                var evtSummary = $"Query validation error: {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", evtSummary);
                return EndpointHelpers.EventResult(false, $"Query validation error: {ex.Message}", "validate_query", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/save-query", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGet(path, out var info) || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var connectionRowKey = root.TryGetProperty("connectionRowKey", out var crk) ? crk.GetString() ?? "" : "";
                var queryText = root.TryGetProperty("queryText", out var qt) ? qt.GetString() ?? "" : "";

                var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var entity = await clientTableService.SaveQueryAsync(partitionKey, connectionRowKey, queryText);

                var evtSummary = "Query saved";
                _ = clientTableService.AppendEventAsync(partitionKey, "save_query", evtSummary);

                return EndpointHelpers.EventResultWithRowKey(true, "Query saved.", entity.RowKey, "save_query", evtSummary);
            }
            catch (Exception ex)
            {
                var pk = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var evtSummary = $"Save query failed: {ex.Message}";
                _ = clientTableService.AppendEventAsync(pk, "save_query", evtSummary);
                return EndpointHelpers.EventResult(false, $"Failed to save query: {ex.Message}", "save_query", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/sample-query", async (string path, HttpRequest request, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
            {
                var notFoundEvtSummary = "Sample query failed: agent not connected";
                return EndpointHelpers.EventResult(false, "Agent not found or not connected.", "sample_query", notFoundEvtSummary);
            }

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            try
            {
                var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
                if (string.IsNullOrWhiteSpace(uniqueId) || !EndpointHelpers.IsDigitsOnly(uniqueId))
                {
                    var missingIdEvtSummary = "Sample query failed";
                    var missingIdDetail = "User unique ID is missing.";
                    _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", missingIdEvtSummary, missingIdDetail);
                    return EndpointHelpers.EventResult(false, missingIdDetail, "sample_query", missingIdEvtSummary, missingIdDetail);
                }

                string queryText = "";
                try
                {
                    using var bodyDoc = JsonDocument.Parse(body);
                    queryText = bodyDoc.RootElement.TryGetProperty("queryText", out var qtEl) ? qtEl.GetString() ?? "" : "";
                }
                catch { }

                var requestBody = EndpointHelpers.AddFieldsToPayload(body, new
                {
                    uniqueId,
                    sqlStatement = $"SELECT TOP 200 * FROM ({queryText}) AS _q"
                });
                var result = await connection.SendCommandAsync("sample_query", requestBody, TimeSpan.FromSeconds(60));

                var querySuccess = true;
                var queryFailMessage = "";
                try
                {
                    using var checkDoc = JsonDocument.Parse(result);
                    if (checkDoc.RootElement.TryGetProperty("success", out var sEl) && !sEl.GetBoolean())
                    {
                        querySuccess = false;
                        queryFailMessage = checkDoc.RootElement.TryGetProperty("message", out var mEl) ? mEl.GetString() ?? "" : "";
                    }
                }
                catch { }

                var queryEvtSummary = querySuccess ? "Sample query completed" : "Sample query failed";
                var queryEvtDetail = querySuccess ? "" : queryFailMessage;
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", queryEvtSummary, queryEvtDetail);
                return Results.Content(
                    EndpointHelpers.InjectEventIntoResult(result, "sample_query", queryEvtSummary, queryEvtDetail),
                    "application/json");
            }
            catch (TimeoutException)
            {
                var evtSummary = "Sample query: timeout";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", evtSummary);
                return EndpointHelpers.EventResult(false, "Agent did not respond within 60 seconds.", "sample_query", evtSummary);
            }
            catch (Exception ex)
            {
                var evtSummary = $"Sample query error: {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", evtSummary);
                return EndpointHelpers.EventResult(false, $"Sample query error: {ex.Message}", "sample_query", evtSummary);
            }
        });

        app.MapGet("/api/agents/{path}/queries", async (string path, string connectionRowKey, AgentRegistry registry, ClientTableService clientTableService) =>
        {
            if (!registry.TryGet(path, out var info) || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var queries = await clientTableService.GetQueriesAsync(partitionKey, connectionRowKey);

            var result = queries.Select(q => new
            {
                rowKey = q.RowKey,
                connectionRowKey = q.ConnectionRowKey,
                queryText = q.QueryText,
                createdAt = q.CreatedAt.ToString("O")
            });

            return Results.Ok(result);
        });
    }
}
