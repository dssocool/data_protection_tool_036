using System.Text.Json;
using Azure.Storage.Blobs;
using DataProtectionTool.ControlCenter.HttpServer.Helpers;
using DataProtectionTool.ControlCenter.HttpServer.Models;
using DataProtectionTool.ControlCenter.HttpServer.Services;

namespace DataProtectionTool.ControlCenter.HttpServer.Endpoints;

public static class EngineEndpoints
{
    public static void MapEngineEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/dp-preview", async (string path, HttpContext httpContext, RpcAgentProxy agentProxy,
            ClientTableService clientTableService, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi, EngineMetadataService metadataService) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            var (connFound, connection) = await agentProxy.GetConnectionAsync(path);
            if (!connFound || connection is null)
            {
                response.StatusCode = 404;
                await response.WriteAsJsonAsync(new { error = "Agent not found or not connected." });
                return;
            }

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            SseWriter.SetupHeaders(response);
            var statusSteps = new List<string>();

            async Task WriteStatus(string msg)
            {
                await SseWriter.WriteEventAsync(response, "status", msg);
                statusSteps.Add(msg);
            }

            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                var root = bodyDoc.RootElement;
                var rowKey = root.GetProperty("rowKey").GetString() ?? "";
                var schema = root.GetProperty("schema").GetString() ?? "";
                var tableName = root.GetProperty("tableName").GetString() ?? "";

                var previewFilenames = ParseStringArray(root, "previewBlobFilenames");
                var previewHeaders = ParseStringArray(root, "previewHeaders");
                var previewColumnTypes = ParseStringArray(root, "previewColumnTypes");

                if (previewFilenames.Count == 0)
                {
                    await SseWriter.WriteErrorAsync(response, "No preview files available. Please preview the table first.");
                    return;
                }

                if (!await EngineRelayService.ValidateEngineConfigAsync(dataEngineConfig, response, requireProfileSetId: true))
                    return;

                var engineBaseUrl = engineApi.BaseUrl;

                await WriteStatus("Copying preview files...");
                var previewContainerClient = blobClient.GetBlobContainerClient(blobConfig.PreviewContainer);
                var engineContainerClient = blobClient.GetBlobContainerClient(blobConfig.Container);

                foreach (var previewFile in previewFilenames)
                {
                    var sourceBlob = previewContainerClient.GetBlobClient(previewFile);
                    var destBlob = engineContainerClient.GetBlobClient(previewFile);
                    using var stream = new MemoryStream();
                    await sourceBlob.DownloadToAsync(stream);
                    stream.Position = 0;
                    await destBlob.UploadAsync(stream, overwrite: true);
                }

                await WriteStatus("Fetching SQL column types...");
                var sqlColumnTypes = new List<string>();
                try
                {
                    var fetchTypesPayload = JsonSerializer.Serialize(new
                    {
                        rowKey,
                        sqlStatement = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION",
                        sqlParams = new Dictionary<string, string> { ["@schema"] = schema, ["@tableName"] = tableName }
                    });
                    var fetchTypesResult = await connection.SendCommandAsync("execute_sql", fetchTypesPayload, TimeSpan.FromSeconds(120));
                    using var fetchTypesDoc = JsonDocument.Parse(fetchTypesResult);
                    var fetchTypesRoot = fetchTypesDoc.RootElement;

                    if (fetchTypesRoot.TryGetProperty("success", out var ftSuccessEl) && ftSuccessEl.GetBoolean()
                        && fetchTypesRoot.TryGetProperty("rows", out var columnsEl) && columnsEl.ValueKind == JsonValueKind.Array)
                    {
                        var typeByName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var col in columnsEl.EnumerateArray())
                        {
                            var colName = col.GetProperty("COLUMN_NAME").GetString() ?? "";
                            var colType = col.GetProperty("DATA_TYPE").GetString() ?? "";
                            if (!string.IsNullOrEmpty(colName))
                                typeByName[colName] = colType;
                        }

                        foreach (var header in previewHeaders)
                        {
                            sqlColumnTypes.Add(typeByName.TryGetValue(header, out var t) ? t : "");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[DryRun] Failed to fetch SQL types: {ex.Message}");
                }

                var connEntityForFormat = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
                DataItemEntity? dataItemForFormat = null;
                string fileFormatId = "";

                if (connEntityForFormat != null)
                {
                    dataItemForFormat = await clientTableService.GetDataItemByTableAsync(
                        partitionKey, connEntityForFormat.ServerName, connEntityForFormat.DatabaseName, schema, tableName);
                    if (dataItemForFormat != null && !string.IsNullOrEmpty(dataItemForFormat.FileFormatId))
                    {
                        fileFormatId = dataItemForFormat.FileFormatId;
                    }
                }

                if (string.IsNullOrEmpty(fileFormatId))
                {
                    await WriteStatus("Creating file format...");
                    var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
                    var blobRef = containerClient.GetBlobClient(previewFilenames[0]);
                    using var downloadStream = new MemoryStream();
                    await blobRef.DownloadToAsync(downloadStream);
                    var fileBytes = downloadStream.ToArray();

                    var (formatSuccess, newFileFormatId, _) = await engineApi.CreateFileFormatAsync(fileBytes, previewFilenames[0]);
                    if (!formatSuccess)
                    {
                        await SseWriter.WriteErrorAsync(response, "File format creation failed.");
                        return;
                    }

                    fileFormatId = newFileFormatId;

                    if (!string.IsNullOrEmpty(fileFormatId) && dataItemForFormat != null)
                    {
                        await clientTableService.UpdateFileFormatIdAsync(dataItemForFormat, fileFormatId);
                    }
                }
                else
                {
                    await WriteStatus("Creating file format... (skipped, already exists)");
                }

                await WriteStatus("Creating file ruleset...");
                var dryRunUuid = Guid.NewGuid().ToString("N");
                var rulesetName = $"ruleset_{dryRunUuid}";

                var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(rulesetName, dataEngineConfig.ConnectorId);
                if (!rulesetSuccess)
                {
                    await SseWriter.WriteErrorAsync(response, "File ruleset creation failed.");
                    return;
                }

                var (metaBatchSuccess, fileMetadataIds) = await EngineRelayService.CreateFileMetadataBatchAsync(
                    engineApi, response, previewFilenames, fileRulesetId, fileFormatId, statusSteps);
                if (!metaBatchSuccess) return;

                await WriteStatus("Creating profile job...");
                using var profileJobResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "profile-jobs", new
                {
                    jobName = $"profile_{dryRunUuid}",
                    profileSetId = int.Parse(dataEngineConfig.ProfileSetId),
                    rulesetId = int.Parse(fileRulesetId)
                });

                if (!(profileJobResp.RootElement.TryGetProperty("success", out var pjSuccessEl) && pjSuccessEl.GetBoolean()))
                {
                    var msg = profileJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job creation failed.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Profile job creation failed.");
                    return;
                }

                var profileJobId = EngineRelayService.ExtractBodyField(profileJobResp, "profileJobId");
                if (string.IsNullOrEmpty(profileJobId))
                {
                    await SseWriter.WriteErrorAsync(response, "Profile job creation returned no profileJobId.");
                    return;
                }

                await WriteStatus("Creating masking job...");
                using var maskingJobResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "masking-jobs", new
                {
                    jobName = $"masking_{dryRunUuid}",
                    rulesetId = int.Parse(fileRulesetId),
                    onTheFlyMasking = false
                });

                if (!(maskingJobResp.RootElement.TryGetProperty("success", out var mjSuccessEl) && mjSuccessEl.GetBoolean()))
                {
                    var msg = maskingJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job creation failed.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Masking job creation failed.");
                    return;
                }

                var maskingJobId = EngineRelayService.ExtractBodyField(maskingJobResp, "maskingJobId");
                if (string.IsNullOrEmpty(maskingJobId))
                {
                    await SseWriter.WriteErrorAsync(response, "Masking job creation returned no maskingJobId.");
                    return;
                }

                await WriteStatus("Running profile job...");
                using var profileExecResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "executions", new
                {
                    jobId = int.Parse(profileJobId)
                });

                if (!(profileExecResp.RootElement.TryGetProperty("success", out var peSuccessEl) && peSuccessEl.GetBoolean()))
                {
                    var msg = profileExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job execution failed to start.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Profile job execution failed to start.");
                    return;
                }

                var profileExecId = EngineRelayService.ExtractBodyField(profileExecResp, "executionId");
                if (string.IsNullOrEmpty(profileExecId))
                {
                    await SseWriter.WriteErrorAsync(response, "Profile job execution returned no executionId.");
                    return;
                }

                var profileStatus = await EngineRelayService.PollExecutionAsync(
                    connection, engineBaseUrl, dataEngineConfig.AuthorizationToken,
                    profileExecId, response, "profile job", statusSteps: statusSteps);

                if (profileStatus is not ("SUCCEEDED" or "WARNING"))
                {
                    await SseWriter.WriteErrorAsync(response, $"Profile job did not succeed. Final status: {profileStatus}");
                    return;
                }

                if (previewHeaders.Count > 0 && previewColumnTypes.Count == previewHeaders.Count)
                {
                    await WriteStatus("Applying mapping rules to column rules...");

                    var sqlTypeByColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < previewHeaders.Count; i++)
                        sqlTypeByColumn[previewHeaders[i]] = previewColumnTypes[i];

                    try
                    {
                        await metadataService.EnsureLoadedAsync();
                        var columnRules = await engineApi.FetchColumnRulesAsync(fileFormatId);
                        var enriched = engineApi.EnrichColumnRules(columnRules, metadataService.Algorithms, metadataService.Domains, metadataService.Frameworks);

                        var algMaskTypes = new Dictionary<string, string>();
                        foreach (var alg in enriched.Algorithms)
                        {
                            var aName = alg.TryGetProperty("algorithmName", out var anEl) ? anEl.GetString() ?? "" : "";
                            var aMaskType = alg.TryGetProperty("maskType", out var mtEl) ? mtEl.GetString() ?? "" : "";
                            if (!string.IsNullOrEmpty(aName))
                                algMaskTypes[aName] = aMaskType;
                        }

                        var fixedCount = 0;
                        foreach (var rule in enriched.Rules)
                        {
                            var fieldName = rule.TryGetProperty("fieldName", out var fnEl) ? fnEl.GetString() ?? "" : "";
                            var algName = rule.TryGetProperty("algorithmName", out var anEl) ? anEl.GetString() ?? "" : "";
                            var metadataId = rule.TryGetProperty("fileFieldMetadataId", out var idEl) ? idEl.ToString() : "";
                            var isMasked = !rule.TryGetProperty("isMasked", out var imEl) || imEl.ValueKind != JsonValueKind.False;

                            if (string.IsNullOrEmpty(fieldName) || string.IsNullOrEmpty(algName)
                                || string.IsNullOrEmpty(metadataId) || !isMasked)
                                continue;

                            if (!sqlTypeByColumn.TryGetValue(fieldName, out var sqlType))
                                continue;

                            var allowedTypes = EndpointHelpers.GetAllowedAlgorithmTypes(sqlType);

                            if (!algMaskTypes.TryGetValue(algName, out var maskType))
                                continue;

                            if (allowedTypes.Contains(maskType))
                                continue;

                            await WriteStatus($"Fixing type mismatch: {fieldName} ({sqlType}) — algorithm type {maskType} not allowed...");
                            using var fixResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "PUT", $"file-field-metadata/{metadataId}", new
                            {
                                isMasked = false,
                                isProfilerWritable = false
                            });
                            fixedCount++;
                        }

                        if (fixedCount > 0)
                            await WriteStatus($"Fixed {fixedCount} column rule(s) with incompatible algorithm types.");
                        else
                            await WriteStatus("All column rules have compatible algorithm types.");
                    }
                    catch (Exception ex)
                    {
                        await WriteStatus($"Warning: Could not apply mapping rules: {ex.Message}");
                    }
                }

                await WriteStatus("Running masking job...");
                using var maskingExecResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "executions", new
                {
                    jobId = int.Parse(maskingJobId)
                });

                if (!(maskingExecResp.RootElement.TryGetProperty("success", out var meSuccessEl) && meSuccessEl.GetBoolean()))
                {
                    var msg = maskingExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job execution failed to start.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Masking job execution failed to start.");
                    return;
                }

                var maskingExecId = EngineRelayService.ExtractBodyField(maskingExecResp, "executionId");
                if (string.IsNullOrEmpty(maskingExecId))
                {
                    await SseWriter.WriteErrorAsync(response, "Masking job execution returned no executionId.");
                    return;
                }

                var maskingStatus = await EngineRelayService.PollExecutionAsync(
                    connection, engineBaseUrl, dataEngineConfig.AuthorizationToken,
                    maskingExecId, response, "masking job", statusSteps: statusSteps);

                if (maskingStatus is not ("SUCCEEDED" or "WARNING"))
                {
                    await SseWriter.WriteErrorAsync(response, $"Masking job did not succeed. Final status: {maskingStatus}");
                    return;
                }

                await WriteStatus("Copying masked results...");
                var maskedFilenames = new List<string>();
                foreach (var previewFile in previewFilenames)
                {
                    var maskedBlob = engineContainerClient.GetBlobClient(previewFile);
                    var maskedName = $"dryrun_{dryRunUuid}_{previewFile}";
                    var destBlob = previewContainerClient.GetBlobClient(maskedName);
                    using var maskedStream = new MemoryStream();
                    await maskedBlob.DownloadToAsync(maskedStream);
                    maskedStream.Position = 0;
                    await destBlob.UploadAsync(maskedStream, overwrite: true);
                    maskedFilenames.Add(maskedName);
                }

                var dryRunEvtSummary = $"DP preview completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
                    $"profileJobId={profileJobId} ({profileStatus}), maskingJobId={maskingJobId} ({maskingStatus})";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview", dryRunEvtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_preview", summary = dryRunEvtSummary, detail = "" }));

                var completeJson = JsonSerializer.Serialize(new
                {
                    success = true,
                    fileFormatId,
                    fileRulesetId,
                    fileMetadataIds,
                    profileJobId,
                    profileStatus,
                    maskingJobId,
                    maskingStatus,
                    maskedFilenames,
                    sqlColumnTypes
                });
                await SseWriter.WriteEventAsync(response, "complete", completeJson);
            }
            catch (TimeoutException)
            {
                var evtSummary = "DP preview: timeout";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview", evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_preview", summary = evtSummary, detail = "" }));
                await SseWriter.WriteErrorAsync(response, "Agent did not respond within 120 seconds.");
            }
            catch (Exception ex)
            {
                var evtSummary = $"DP preview error: {ex.Message}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview", evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_preview", summary = evtSummary, detail = "" }));
                await SseWriter.WriteErrorAsync(response, $"DP preview error: {ex.Message}");
            }
        });

        app.MapPost("/api/agents/{path}/dp-run", async (string path, HttpContext httpContext, RpcAgentProxy agentProxy,
            ClientTableService clientTableService, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            var (connFound, connection) = await agentProxy.GetConnectionAsync(path);
            if (!connFound || connection is null)
            {
                response.StatusCode = 404;
                await response.WriteAsJsonAsync(new { error = "Agent not found or not connected." });
                return;
            }

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            SseWriter.SetupHeaders(response);
            var statusSteps = new List<string>();

            async Task WriteStatus(string msg)
            {
                await SseWriter.WriteEventAsync(response, "status", msg);
                statusSteps.Add(msg);
            }

            var flowId = "";
            using (var preDoc = JsonDocument.Parse(body))
            {
                var flowRowKey = preDoc.RootElement.TryGetProperty("flowRowKey", out var frEl) ? frEl.GetString() ?? "" : "";
                flowId = flowRowKey.StartsWith("flow_") ? flowRowKey["flow_".Length..] : flowRowKey;
            }

            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                var root = bodyDoc.RootElement;
                var rowKey = root.GetProperty("rowKey").GetString() ?? "";
                var schema = root.GetProperty("schema").GetString() ?? "";
                var tableName = root.GetProperty("tableName").GetString() ?? "";
                var destConnectionRowKey = root.TryGetProperty("destConnectionRowKey", out var dcrEl) ? dcrEl.GetString() ?? "" : "";
                var destSchema = root.TryGetProperty("destSchema", out var dsEl) ? dsEl.GetString() ?? "" : "";

                if (string.IsNullOrEmpty(destConnectionRowKey) || string.IsNullOrEmpty(destSchema))
                {
                    await SseWriter.WriteErrorAsync(response, "Destination connection and schema are required.");
                    return;
                }

                if (!await EngineRelayService.ValidateEngineConfigAsync(dataEngineConfig, response))
                    return;

                var engineBaseUrl = engineApi.BaseUrl;

                var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
                if (connEntity == null)
                {
                    await SseWriter.WriteErrorAsync(response, "Connection not found.");
                    return;
                }

                var dataItem = await clientTableService.GetDataItemByTableAsync(
                    partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tableName);

                var fileFormatId = dataItem != null ? dataItem.FileFormatId : "";
                if (string.IsNullOrEmpty(fileFormatId))
                {
                    await SseWriter.WriteErrorAsync(response, "File format not found. Please run Dry Run first.");
                    return;
                }

                await WriteStatus("Exporting full table...");
                var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
                if (string.IsNullOrWhiteSpace(uniqueId) || !EndpointHelpers.IsDigitsOnly(uniqueId))
                {
                    await SseWriter.WriteErrorAsync(response, "User unique ID is missing.");
                    return;
                }

                var exportPayload = JsonSerializer.Serialize(new
                {
                    rowKey, schema, tableName, uniqueId,
                    sqlStatement = $"SELECT * FROM [{schema}].[{tableName}]",
                    filePrefix = "fullrun",
                    containerName = blobConfig.Container
                });
                var exportResult = await connection.SendCommandAsync("export_table", exportPayload, TimeSpan.FromSeconds(600));

                using var exportDoc = JsonDocument.Parse(exportResult);
                var exportRoot = exportDoc.RootElement;

                if (!(exportRoot.TryGetProperty("success", out var exportSuccessEl) && exportSuccessEl.GetBoolean()))
                {
                    var msg = exportRoot.TryGetProperty("message", out var mEl) ? mEl.GetString() : "Export failed.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Export failed.");
                    return;
                }

                var exportFilenames = ParseStringArray(exportRoot, "filenames");

                if (exportFilenames.Count == 0)
                {
                    await SseWriter.WriteErrorAsync(response, "Export produced no files.");
                    return;
                }

                var exportEvtSummary = $"DP run: exported {exportFilenames.Count} file(s) for {schema}.{tableName}";
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, exportEvtSummary);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_run", summary = exportEvtSummary, detail = "" }));

                await WriteStatus("Creating file ruleset...");
                var fullRunUuid = Guid.NewGuid().ToString("N");

                var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(
                    $"fullrun_ruleset_{fullRunUuid}", dataEngineConfig.ConnectorId);
                if (!rulesetSuccess)
                {
                    await SseWriter.WriteErrorAsync(response, "File ruleset creation failed.");
                    return;
                }

                var (metaBatchSuccess, fileMetadataIds) = await EngineRelayService.CreateFileMetadataBatchAsync(
                    engineApi, response, exportFilenames, fileRulesetId, fileFormatId, statusSteps);
                if (!metaBatchSuccess) return;

                await WriteStatus("Creating masking job...");
                using var maskingJobResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "masking-jobs", new
                {
                    jobName = $"fullrun_masking_{fullRunUuid}",
                    rulesetId = int.Parse(fileRulesetId),
                    onTheFlyMasking = false
                });

                if (!(maskingJobResp.RootElement.TryGetProperty("success", out var mjSuccessEl) && mjSuccessEl.GetBoolean()))
                {
                    var msg = maskingJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job creation failed.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Masking job creation failed.");
                    return;
                }

                var maskingJobId = EngineRelayService.ExtractBodyField(maskingJobResp, "maskingJobId");
                if (string.IsNullOrEmpty(maskingJobId))
                {
                    await SseWriter.WriteErrorAsync(response, "Masking job creation returned no maskingJobId.");
                    return;
                }

                await WriteStatus("Executing masking job...");
                using var maskingExecResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "executions", new
                {
                    jobId = int.Parse(maskingJobId)
                });

                if (!(maskingExecResp.RootElement.TryGetProperty("success", out var meSuccessEl) && meSuccessEl.GetBoolean()))
                {
                    var msg = maskingExecResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job execution failed to start.";
                    await SseWriter.WriteErrorAsync(response, msg ?? "Masking job execution failed to start.");
                    return;
                }

                var maskingExecId = EngineRelayService.ExtractBodyField(maskingExecResp, "executionId");
                if (string.IsNullOrEmpty(maskingExecId))
                {
                    await SseWriter.WriteErrorAsync(response, "Masking job execution returned no executionId.");
                    return;
                }

                await WriteStatus("Waiting for masking to complete...");
                var maskingStatus = await EngineRelayService.PollExecutionAsync(
                    connection, engineBaseUrl, dataEngineConfig.AuthorizationToken,
                    maskingExecId, response, "", maxIterations: 600, statusSteps: statusSteps);

                if (maskingStatus is not ("SUCCEEDED" or "WARNING"))
                {
                    await SseWriter.WriteErrorAsync(response, $"Masking job did not succeed. Final status: {maskingStatus}");
                    return;
                }

                await WriteStatus("Loading masked data to destination...");
                for (var fi = 0; fi < exportFilenames.Count; fi++)
                {
                    var exportFile = exportFilenames[fi];
                    await WriteStatus($"Loading masked file to destination... ({fi + 1} of {exportFilenames.Count})");

                    var loadPayload = JsonSerializer.Serialize(new
                    {
                        destRowKey = destConnectionRowKey,
                        destSchema,
                        tableName,
                        blobFilename = exportFile,
                        createTable = fi == 0,
                        truncate = fi == 0,
                        containerName = blobConfig.Container
                    });

                    var loadResult = await connection.SendCommandAsync("load_masked_to_table", loadPayload, TimeSpan.FromSeconds(600));

                    using var loadDoc = JsonDocument.Parse(loadResult);
                    var loadRoot = loadDoc.RootElement;

                    if (!(loadRoot.TryGetProperty("success", out var loadSuccessEl) && loadSuccessEl.GetBoolean()))
                    {
                        var loadMsg = loadRoot.TryGetProperty("message", out var lmEl) ? lmEl.GetString() : $"Failed to load masked file {exportFile} to destination.";
                        await SseWriter.WriteErrorAsync(response, loadMsg ?? $"Failed to load masked file {exportFile} to destination.");
                        return;
                    }
                }

                var fullRunEvtSummary = $"DP run completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
                    $"maskingJobId={maskingJobId} ({maskingStatus}), files={exportFilenames.Count}, " +
                    $"destination=[{destSchema}].{tableName}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, fullRunEvtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_run", summary = fullRunEvtSummary, detail = "" }));

                var completeJson = JsonSerializer.Serialize(new
                {
                    success = true,
                    fileFormatId,
                    fileRulesetId,
                    fileMetadataIds,
                    maskingJobId,
                    maskingStatus,
                    exportFilenames
                });
                await SseWriter.WriteEventAsync(response, "complete", completeJson);
            }
            catch (TimeoutException)
            {
                var evtSummary = "DP run: timeout";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_run", summary = evtSummary, detail = "" }));
                await SseWriter.WriteErrorAsync(response, "Agent did not respond in time.");
            }
            catch (Exception ex)
            {
                var evtSummary = $"DP run error: {ex.Message}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_run", summary = evtSummary, detail = "" }));
                await SseWriter.WriteErrorAsync(response, $"DP run error: {ex.Message}");
            }
        });

        app.MapGet("/api/agents/{path}/column-rules", async (string path, string? fileFormatId, RpcAgentProxy agentProxy, EngineApiClient engineApi, EngineMetadataService metadataService) =>
        {
            if (!await agentProxy.TryGetAsync(path))
                return Results.NotFound(new { error = "Agent not found." });

            if (string.IsNullOrWhiteSpace(fileFormatId))
                return Results.Ok(new { success = false, message = "fileFormatId is required." });

            try
            {
                await metadataService.EnsureLoadedAsync();
                var rules = await engineApi.FetchColumnRulesAsync(fileFormatId);
                var enriched = engineApi.EnrichColumnRules(rules, metadataService.Algorithms, metadataService.Domains, metadataService.Frameworks);

                return Results.Ok(new
                {
                    success = true,
                    fixedCount = 0,
                    responseList = enriched.Rules.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
                    algorithms = enriched.Algorithms.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
                    domains = enriched.Domains.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
                    frameworks = enriched.Frameworks.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray()
                });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Column rules fetch error: {ex.Message}" });
            }
        });

        app.MapPut("/api/agents/{path}/column-rule/{fileFieldMetadataId}", async (string path, string fileFieldMetadataId, HttpRequest request, RpcAgentProxy agentProxy, DataEngineConfig dataEngineConfig) =>
        {
            var (connFound, connection) = await agentProxy.GetConnectionAsync(path);
            if (!connFound || connection is null)
                return Results.NotFound(new { error = "Agent not found or not connected." });

            if (string.IsNullOrWhiteSpace(fileFieldMetadataId))
                return Results.Ok(new { success = false, message = "fileFieldMetadataId is required." });

            var body = await request.ReadBodyAsync();

            using var bodyDoc = JsonDocument.Parse(body);
            var algorithmName = bodyDoc.RootElement.TryGetProperty("algorithmName", out var algEl) ? algEl.GetString() ?? "" : "";
            var domainName = bodyDoc.RootElement.TryGetProperty("domainName", out var domEl) ? domEl.GetString() ?? "" : "";

            var engineBaseUrl = $"{dataEngineConfig.EngineUrl.TrimEnd('/')}/masking/api/v5.1.44";

            var httpPayload = JsonSerializer.Serialize(new
            {
                method = "PUT",
                url = $"{engineBaseUrl}/file-field-metadata/{fileFieldMetadataId}",
                headers = new Dictionary<string, string>
                {
                    ["accept"] = "application/json",
                    ["Authorization"] = dataEngineConfig.AuthorizationToken,
                    ["Content-Type"] = "application/json"
                },
                body = JsonSerializer.Serialize(new
                {
                    algorithmName,
                    domainName,
                    isProfilerWritable = false
                })
            });

            try
            {
                var result = await connection.SendCommandAsync("http_request", httpPayload, TimeSpan.FromSeconds(120));
                return Results.Content(result, "application/json");
            }
            catch (TimeoutException)
            {
                return Results.Ok(new { success = false, message = "Agent did not respond within 120 seconds." });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Column rule save error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/engine-metadata", async (string path, RpcAgentProxy agentProxy, EngineMetadataService metadataService) =>
        {
            if (!await agentProxy.TryGetAsync(path))
                return Results.NotFound(new { error = "Agent not found." });

            try
            {
                await metadataService.EnsureLoadedAsync();

                return Results.Ok(new
                {
                    success = true,
                    algorithms = metadataService.Algorithms?.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray() ?? Array.Empty<object?>(),
                    domains = metadataService.Domains?.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray() ?? Array.Empty<object?>(),
                    frameworks = metadataService.Frameworks?.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray() ?? Array.Empty<object?>()
                });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Engine metadata fetch error: {ex.Message}" });
            }
        });

        app.MapGet("/api/allowed-algorithm-types", (string sqlType) =>
        {
            var allowed = EndpointHelpers.GetAllowedAlgorithmTypes(sqlType);
            return Results.Ok(new { success = true, allowedTypes = allowed });
        });
    }

    private static List<string> ParseStringArray(JsonElement root, string propertyName)
    {
        var list = new List<string>();
        if (root.TryGetProperty(propertyName, out var el) && el.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in el.EnumerateArray())
            {
                var val = item.GetString();
                if (!string.IsNullOrEmpty(val))
                    list.Add(val);
            }
        }
        return list;
    }
}
