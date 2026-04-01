using System.Text.Json;
using DataProtectionTool.ControlCenter.HttpServer.Helpers;
using DataProtectionTool.ControlCenter.HttpServer.Models;
using DataProtectionTool.ControlCenter.HttpServer.Services;

namespace DataProtectionTool.ControlCenter.HttpServer.Endpoints;

public static class FlowEndpoints
{
    public static void MapFlowEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/agents/{path}/events", async (string path, RpcAgentProxy agentProxy, ClientTableService clientTableService) =>
        {
            var (found, info) = await agentProxy.GetAgentInfoAsync(path);
            if (!found || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var events = await clientTableService.GetEventsAsync(partitionKey);

            var result = events.Select(e =>
            {
                string[] steps = Array.Empty<string>();
                var detail = e.Detail;
                if (!string.IsNullOrEmpty(detail) && detail.TrimStart().StartsWith("["))
                {
                    try
                    {
                        steps = JsonSerializer.Deserialize<string[]>(detail) ?? Array.Empty<string>();
                        detail = "";
                    }
                    catch (JsonException) { }
                }
                return new
                {
                    timestamp = e.Timestamp.ToString("O"),
                    type = e.Type,
                    flowId = e.FlowId,
                    summary = e.Summary,
                    detail,
                    steps
                };
            });

            return Results.Ok(result);
        });

        app.MapPost("/api/agents/{path}/save-flow", async (string path, HttpRequest request, RpcAgentProxy agentProxy, ClientTableService clientTableService) =>
        {
            var (found, info) = await agentProxy.GetAgentInfoAsync(path);
            if (!found || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var sourceJson = root.TryGetProperty("sourceJson", out var sj) ? sj.GetString() ?? "" : "";
                var destJson = root.TryGetProperty("destJson", out var dj) ? dj.GetString() ?? "" : "";

                var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var entity = await clientTableService.SaveFlowAsync(partitionKey, sourceJson, destJson);

                return Results.Ok(new { success = true, rowKey = entity.RowKey });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Failed to save flow: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/flows", async (string path, RpcAgentProxy agentProxy, ClientTableService clientTableService) =>
        {
            var (found, info) = await agentProxy.GetAgentInfoAsync(path);
            if (!found || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var flows = await clientTableService.GetFlowsAsync(partitionKey);

            var result = flows.Select(f => new
            {
                rowKey = f.RowKey,
                sourceJson = f.SourceJson,
                destJson = f.DestJson,
                createdAt = f.CreatedAt.ToString("O")
            });

            return Results.Ok(result);
        });

        app.MapPost("/api/agents/{path}/delete-flows", async (string path, HttpRequest request, RpcAgentProxy agentProxy, ClientTableService clientTableService) =>
        {
            var (found, info) = await agentProxy.GetAgentInfoAsync(path);
            if (!found || info is null)
                return Results.NotFound(new { error = "Agent not found." });

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var rowKeys = new List<string>();
                if (root.TryGetProperty("rowKeys", out var rk) && rk.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in rk.EnumerateArray())
                    {
                        var val = item.GetString();
                        if (!string.IsNullOrEmpty(val))
                            rowKeys.Add(val);
                    }
                }

                if (rowKeys.Count == 0)
                    return Results.Ok(new { success = false, message = "No rowKeys provided." });

                var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var deleted = await clientTableService.DeleteFlowsAsync(partitionKey, rowKeys);

                return Results.Ok(new { success = true, deleted });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Failed to delete flows: {ex.Message}" });
            }
        });
    }
}
