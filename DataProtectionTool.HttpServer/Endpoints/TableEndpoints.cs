using System.Text.Json;
using Azure.Storage.Blobs;
using DataProtectionTool.HttpServer.Helpers;
using DataProtectionTool.HttpServer.Models;
using DataProtectionTool.HttpServer.Services;

namespace DataProtectionTool.HttpServer.Endpoints;

public static class TableEndpoints
{
    public static void MapTableEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/sample-table", async (string path, HttpRequest request,
            RpcAgentProxy agentProxy, ClientTableService clientTableService,
            BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            return await SampleTableCoreAsync(path, request, agentProxy, clientTableService, blobClient, blobStorageConfig, useCache: true, labelPrefix: "Sample table");
        });

        app.MapPost("/api/agents/{path}/reload-sample-table", async (string path, HttpRequest request,
            RpcAgentProxy agentProxy, ClientTableService clientTableService,
            BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            return await SampleTableCoreAsync(path, request, agentProxy, clientTableService, blobClient, blobStorageConfig, useCache: false, labelPrefix: "Reload sample table");
        });
    }

    private static async Task<IResult> SampleTableCoreAsync(
        string path, HttpRequest request,
        RpcAgentProxy agentProxy, ClientTableService clientTableService,
        BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig,
        bool useCache, string labelPrefix)
    {
        var (connFound, connection) = await agentProxy.GetConnectionAsync(path);
        if (!connFound || connection is null)
        {
            var notFoundEvtSummary = $"{labelPrefix} failed: agent not connected";
            return EndpointHelpers.EventResult(false, "Agent not found or not connected.", "sample_table", notFoundEvtSummary);
        }

        var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
        var body = await request.ReadBodyAsync();

        string connRowKey = "", schema = "", tName = "";
        try
        {
            using var bodyDoc = JsonDocument.Parse(body);
            connRowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
            schema = bodyDoc.RootElement.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
            tName = bodyDoc.RootElement.TryGetProperty("tableName", out var tEl) ? tEl.GetString() ?? "" : "";
        }
        catch
        {
            if (!useCache)
            {
                var invalidBodyEvtSummary = $"{labelPrefix} failed: invalid request body";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", invalidBodyEvtSummary);
                return EndpointHelpers.EventResult(false, "Invalid request body.", "sample_table", invalidBodyEvtSummary);
            }
        }

        var tableLabel = $"{schema}.{tName}";

        var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, connRowKey);
        DataItemEntity? dataItem = null;

        if (useCache && connEntity != null)
        {
            dataItem = await clientTableService.GetDataItemByTableAsync(
                partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tName);

            if (dataItem != null && !string.IsNullOrEmpty(dataItem.PreviewFileList))
            {
                var cachedFilenames = dataItem.PreviewFileList.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
                var allBlobsExist = true;
                try
                {
                    var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
                    foreach (var fn in cachedFilenames)
                    {
                        if (!await containerClient.GetBlobClient(fn).ExistsAsync())
                        {
                            allBlobsExist = false;
                            break;
                        }
                    }
                }
                catch
                {
                    allBlobsExist = false;
                }

                if (allBlobsExist)
                {
                    var evtSummary = $"Sample table (cached): {tableLabel}";
                    _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary);
                    return Results.Ok(new
                    {
                        success = true,
                        filenames = cachedFilenames,
                        cached = true,
                        @event = EndpointHelpers.EventPayload("sample_table", evtSummary)
                    });
                }

                await clientTableService.UpdatePreviewFileListAsync(dataItem, "");
            }
        }
        else if (!useCache && connEntity != null)
        {
            dataItem = await clientTableService.GetDataItemByTableAsync(
                partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tName);
        }
        else if (!useCache && connEntity == null)
        {
            return Results.NotFound(new { error = "Connection not found." });
        }

        try
        {
            var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
            if (string.IsNullOrWhiteSpace(uniqueId) || !EndpointHelpers.IsDigitsOnly(uniqueId))
            {
                var missingIdEvtSummary = $"{labelPrefix} failed: {tableLabel}";
                var missingIdDetail = "User unique ID is missing.";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", missingIdEvtSummary, missingIdDetail);
                return EndpointHelpers.EventResult(false, missingIdDetail, "sample_table", missingIdEvtSummary, missingIdDetail);
            }

            var requestBody = EndpointHelpers.AddFieldsToPayload(body, new { uniqueId, sqlStatement = $"SELECT * FROM [{schema}].[{tName}] TABLESAMPLE (200 ROWS)" });
            var result = await connection.SendCommandAsync("sample_table", requestBody, TimeSpan.FromSeconds(60));

            if (dataItem != null)
            {
                try
                {
                    using var resultDoc = JsonDocument.Parse(result);
                    if (resultDoc.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean())
                    {
                        var filenames = new List<string>();
                        if (resultDoc.RootElement.TryGetProperty("filenames", out var fnEl) && fnEl.ValueKind == JsonValueKind.Array)
                            filenames = fnEl.EnumerateArray().Select(e => e.GetString() ?? "").Where(f => f != "").ToList();
                        else if (resultDoc.RootElement.TryGetProperty("filename", out var fEl))
                            filenames = new List<string> { fEl.GetString() ?? "" };

                        if (filenames.Count > 0)
                        {
                            _ = clientTableService.UpdatePreviewFileListAsync(dataItem, string.Join(",", filenames));
                        }
                    }
                }
                catch { }
            }

            var opSuccess = true;
            var failMessage = "";
            try
            {
                using var checkDoc = JsonDocument.Parse(result);
                if (checkDoc.RootElement.TryGetProperty("success", out var sEl) && !sEl.GetBoolean())
                {
                    opSuccess = false;
                    failMessage = checkDoc.RootElement.TryGetProperty("message", out var mEl) ? mEl.GetString() ?? "" : "";
                }
            }
            catch { }

            var evtSummary2 = opSuccess
                ? $"{labelPrefix}: {tableLabel}"
                : $"{labelPrefix} failed: {tableLabel}";
            var evtDetail = opSuccess ? "" : failMessage;
            _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary2, evtDetail);
            return Results.Content(
                EndpointHelpers.InjectEventIntoResult(result, "sample_table", evtSummary2, evtDetail),
                "application/json");
        }
        catch (TimeoutException)
        {
            var evtSummary = $"{labelPrefix} timeout: {tableLabel}";
            _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary);
            return EndpointHelpers.EventResult(false, "Agent did not respond within 60 seconds.", "sample_table", evtSummary);
        }
        catch (Exception ex)
        {
            var evtSummary = $"{labelPrefix} error: {ex.Message}";
            _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary);
            return EndpointHelpers.EventResult(false, $"{labelPrefix} error: {ex.Message}", "sample_table", evtSummary);
        }
    }
}
