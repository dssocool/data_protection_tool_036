using System.Text.Json;
using Azure.Storage.Blobs;
using DataProtectionTool.ControlCenter.Helpers;
using DataProtectionTool.ControlCenter.Models;
using DataProtectionTool.ControlCenter.Services;

namespace DataProtectionTool.ControlCenter.Endpoints;

public static class EngineEndpoints
{
    public static void MapEngineEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/dp-preview", async (string path, HttpContext httpContext, AgentRegistry registry,
            ClientTableService clientTableService, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi, EngineMetadataService metadataService) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            if (!registry.TryGetConnection(path, out var connection) || connection is null)
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

                // Copy preview files from data_preview container to configured (engine) container
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

                // Fetch original SQL types from the database via the agent
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

                // Get or create file format
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

                // Create a file ruleset
                await WriteStatus("Creating file ruleset...");
                var dryRunUuid = Guid.NewGuid().ToString("N");
                var rulesetName = $"ruleset_{dryRunUuid}";

                var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(rulesetName, dataEngineConfig.ConnectorId);
                if (!rulesetSuccess)
                {
                    await SseWriter.WriteErrorAsync(response, "File ruleset creation failed.");
                    return;
                }

                // Create file metadata for each preview file
                var (metaBatchSuccess, fileMetadataIds) = await EngineRelayService.CreateFileMetadataBatchAsync(
                    engineApi, response, previewFilenames, fileRulesetId, fileFormatId, statusSteps);
                if (!metaBatchSuccess) return;

                // Create profile job
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

                // Create masking job
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

                // Run the profile job
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

                // Poll profile job status
                var profileStatus = await EngineRelayService.PollExecutionAsync(
                    connection, engineBaseUrl, dataEngineConfig.AuthorizationToken,
                    profileExecId, response, "profile job", statusSteps: statusSteps);

                if (profileStatus is not ("SUCCEEDED" or "WARNING"))
                {
                    await SseWriter.WriteErrorAsync(response, $"Profile job did not succeed. Final status: {profileStatus}");
                    return;
                }

                // Apply mapping rules to fix column rules with incompatible algorithm types
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

                // Run the masking job
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

                // Poll masking job status
                var maskingStatus = await EngineRelayService.PollExecutionAsync(
                    connection, engineBaseUrl, dataEngineConfig.AuthorizationToken,
                    maskingExecId, response, "masking job", statusSteps: statusSteps);

                if (maskingStatus is not ("SUCCEEDED" or "WARNING"))
                {
                    await SseWriter.WriteErrorAsync(response, $"Masking job did not succeed. Final status: {maskingStatus}");
                    return;
                }

                // Copy masked files from engine container back to preview container
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

        app.MapPost("/api/agents/{path}/dp-preview-multi", async (string path, HttpContext httpContext, AgentRegistry registry,
            ClientTableService clientTableService, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi, EngineMetadataService metadataService) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            if (!registry.TryGetConnection(path, out var connection) || connection is null)
            {
                response.StatusCode = 404;
                await response.WriteAsJsonAsync(new { error = "Agent not found or not connected." });
                return;
            }

            var partitionKey = ClientEntity.BuildPartitionKey(connection.Info.Oid, connection.Info.Tid);
            var body = await request.ReadBodyAsync();

            SseWriter.SetupHeaders(response);
            var statusSteps = new List<string>();
            var sseLock = new SemaphoreSlim(1, 1);

            async Task WriteStatus(string msg)
            {
                await EngineRelayService.WriteStatusThreadSafeAsync(response, sseLock, statusSteps, msg);
            }

            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                var root = bodyDoc.RootElement;

                if (!root.TryGetProperty("tables", out var tablesEl) || tablesEl.ValueKind != JsonValueKind.Array)
                {
                    await SseWriter.WriteErrorAsync(response, "Request must include a 'tables' array.");
                    return;
                }

                var tables = new List<(string rowKey, string schema, string tableName)>();
                foreach (var tEl in tablesEl.EnumerateArray())
                {
                    var rk = tEl.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
                    var sc = tEl.TryGetProperty("schema", out var scEl) ? scEl.GetString() ?? "" : "";
                    var tn = tEl.TryGetProperty("tableName", out var tnEl) ? tnEl.GetString() ?? "" : "";
                    if (!string.IsNullOrEmpty(rk) && !string.IsNullOrEmpty(sc) && !string.IsNullOrEmpty(tn))
                        tables.Add((rk, sc, tn));
                }

                if (tables.Count == 0)
                {
                    await SseWriter.WriteErrorAsync(response, "No valid tables provided.");
                    return;
                }

                if (!await EngineRelayService.ValidateEngineConfigAsync(dataEngineConfig, response, requireProfileSetId: true))
                    return;

                var engineBaseUrl = engineApi.BaseUrl;
                var previewContainerClient = blobClient.GetBlobContainerClient(blobConfig.PreviewContainer);
                var engineContainerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
                var dryRunUuid = Guid.NewGuid().ToString("N");

                // ---- Group 2: create ruleset + profile job + masking job (runs once, independent of Group 1) ----
                var group2Tcs = new TaskCompletionSource<(string fileRulesetId, string profileJobId, string maskingJobId)>(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                var group2Task = Task.Run(async () =>
                {
                    try
                    {
                        await WriteStatus("Creating file ruleset...");
                        var rulesetName = $"ruleset_{dryRunUuid}";
                        var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(rulesetName, dataEngineConfig.ConnectorId);
                        if (!rulesetSuccess)
                        {
                            group2Tcs.TrySetException(new InvalidOperationException("File ruleset creation failed."));
                            return;
                        }

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
                            group2Tcs.TrySetException(new InvalidOperationException(msg ?? "Profile job creation failed."));
                            return;
                        }

                        var profileJobId = EngineRelayService.ExtractBodyField(profileJobResp, "profileJobId");
                        if (string.IsNullOrEmpty(profileJobId))
                        {
                            group2Tcs.TrySetException(new InvalidOperationException("Profile job creation returned no profileJobId."));
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
                            group2Tcs.TrySetException(new InvalidOperationException(msg ?? "Masking job creation failed."));
                            return;
                        }

                        var maskingJobId = EngineRelayService.ExtractBodyField(maskingJobResp, "maskingJobId");
                        if (string.IsNullOrEmpty(maskingJobId))
                        {
                            group2Tcs.TrySetException(new InvalidOperationException("Masking job creation returned no maskingJobId."));
                            return;
                        }

                        group2Tcs.TrySetResult((fileRulesetId, profileJobId, maskingJobId));
                    }
                    catch (Exception ex)
                    {
                        group2Tcs.TrySetException(ex);
                    }
                });

                // ---- Per-table result holder ----
                var tableResults = new TablePrepResult[tables.Count];

                // ---- Group 1 + Group 3 pipeline per table (parallel across tables) ----
                var perTableTasks = tables.Select((table, idx) => Task.Run(async () =>
                {
                    var (rowKey, schema, tableName) = table;
                    var tableLabel = $"{schema}.{tableName}";

                    // -- Group 1: sample, copy blobs, fetch SQL types, create file format --
                    await WriteStatus($"[{tableLabel}] Sampling table...");
                    var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
                    var samplePayload = JsonSerializer.Serialize(new
                    {
                        rowKey, schema, tableName, uniqueId,
                        sqlStatement = $"SELECT * FROM [{schema}].[{tableName}] TABLESAMPLE (200 ROWS)"
                    });
                    var sampleResult = await connection.SendCommandAsync("sample_table", samplePayload, TimeSpan.FromSeconds(120));
                    using var sampleDoc = JsonDocument.Parse(sampleResult);
                    var sampleRoot = sampleDoc.RootElement;

                    if (!(sampleRoot.TryGetProperty("success", out var ssEl) && ssEl.GetBoolean()))
                    {
                        var msg = sampleRoot.TryGetProperty("message", out var mEl) ? mEl.GetString() : "Sample failed.";
                        throw new InvalidOperationException($"[{tableLabel}] Sample failed: {msg}");
                    }

                    var filenames = new List<string>();
                    if (sampleRoot.TryGetProperty("filenames", out var fnEl) && fnEl.ValueKind == JsonValueKind.Array)
                        filenames = fnEl.EnumerateArray().Select(e => e.GetString() ?? "").Where(f => f != "").ToList();
                    else if (sampleRoot.TryGetProperty("filename", out var fEl))
                    {
                        var fn = fEl.GetString() ?? "";
                        if (!string.IsNullOrEmpty(fn)) filenames.Add(fn);
                    }

                    if (filenames.Count == 0)
                        throw new InvalidOperationException($"[{tableLabel}] Sample produced no files.");

                    // Copy preview blobs to engine container
                    await WriteStatus($"[{tableLabel}] Copying preview files...");
                    foreach (var previewFile in filenames)
                    {
                        var sourceBlob = previewContainerClient.GetBlobClient(previewFile);
                        var destBlob = engineContainerClient.GetBlobClient(previewFile);
                        using var stream = new MemoryStream();
                        await sourceBlob.DownloadToAsync(stream);
                        stream.Position = 0;
                        await destBlob.UploadAsync(stream, overwrite: true);
                    }

                    // Fetch SQL column types
                    await WriteStatus($"[{tableLabel}] Fetching SQL column types...");
                    var previewHeaders = new List<string>();
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
                            foreach (var col in columnsEl.EnumerateArray())
                            {
                                var colName = col.GetProperty("COLUMN_NAME").GetString() ?? "";
                                var colType = col.GetProperty("DATA_TYPE").GetString() ?? "";
                                if (!string.IsNullOrEmpty(colName))
                                {
                                    previewHeaders.Add(colName);
                                    sqlColumnTypes.Add(colType);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"[DpPreviewMulti] Failed to fetch SQL types for {tableLabel}: {ex.Message}");
                    }

                    // Get or create file format
                    var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
                    DataItemEntity? dataItem = null;
                    string fileFormatId = "";

                    if (connEntity != null)
                    {
                        dataItem = await clientTableService.GetDataItemByTableAsync(
                            partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tableName);
                        if (dataItem != null && !string.IsNullOrEmpty(dataItem.FileFormatId))
                            fileFormatId = dataItem.FileFormatId;
                    }

                    if (string.IsNullOrEmpty(fileFormatId))
                    {
                        await WriteStatus($"[{tableLabel}] Creating file format...");
                        var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
                        var blobRef = containerClient.GetBlobClient(filenames[0]);
                        using var downloadStream = new MemoryStream();
                        await blobRef.DownloadToAsync(downloadStream);
                        var fileBytes = downloadStream.ToArray();

                        var (formatSuccess, newFileFormatId, _) = await engineApi.CreateFileFormatAsync(fileBytes, filenames[0]);
                        if (!formatSuccess)
                            throw new InvalidOperationException($"[{tableLabel}] File format creation failed.");

                        fileFormatId = newFileFormatId;
                        if (!string.IsNullOrEmpty(fileFormatId) && dataItem != null)
                            await clientTableService.UpdateFileFormatIdAsync(dataItem, fileFormatId);
                    }
                    else
                    {
                        await WriteStatus($"[{tableLabel}] File format already exists.");
                    }

                    // -- Group 3: wait for Group 2, then create file metadata --
                    var (rulesetId, _, _) = await group2Tcs.Task;

                    await WriteStatus($"[{tableLabel}] Creating file metadata...");
                    var allMetadataIds = new List<string>();
                    for (var fi = 0; fi < filenames.Count; fi++)
                    {
                        var (metaSuccess, fileMetadataId, _) = await engineApi.CreateFileMetadataAsync(filenames[fi], rulesetId, fileFormatId);
                        if (!metaSuccess)
                            throw new InvalidOperationException($"[{tableLabel}] File metadata creation failed for {filenames[fi]}.");
                        allMetadataIds.Add(fileMetadataId);
                    }

                    await WriteStatus($"[{tableLabel}] Table preparation complete.");

                    tableResults[idx] = new TablePrepResult
                    {
                        RowKey = rowKey,
                        Schema = schema,
                        TableName = tableName,
                        Filenames = filenames,
                        FileFormatId = fileFormatId,
                        PreviewHeaders = previewHeaders,
                        SqlColumnTypes = sqlColumnTypes,
                        FileMetadataIds = allMetadataIds,
                    };
                })).ToArray();

                // Await all per-table tasks (Groups 1+3) and Group 2
                await Task.WhenAll(perTableTasks);
                await group2Task;

                var (g2RulesetId, g2ProfileJobId, g2MaskingJobId) = await group2Tcs.Task;

                // ---- Group 4: run profile, fix mappings, run masking, copy results ----
                await WriteStatus("Running profile job...");
                using var profileExecResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "executions", new
                {
                    jobId = int.Parse(g2ProfileJobId)
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

                // Apply mapping rules per table
                await metadataService.EnsureLoadedAsync();
                foreach (var tr in tableResults)
                {
                    if (tr.PreviewHeaders.Count == 0 || tr.SqlColumnTypes.Count != tr.PreviewHeaders.Count)
                        continue;

                    var tableLabel = $"{tr.Schema}.{tr.TableName}";
                    await WriteStatus($"[{tableLabel}] Applying mapping rules...");

                    var sqlTypeByColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < tr.PreviewHeaders.Count; i++)
                        sqlTypeByColumn[tr.PreviewHeaders[i]] = tr.SqlColumnTypes[i];

                    try
                    {
                        var columnRules = await engineApi.FetchColumnRulesAsync(tr.FileFormatId);
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

                            await WriteStatus($"[{tableLabel}] Fixing type mismatch: {fieldName} ({sqlType})...");
                            using var fixResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "PUT", $"file-field-metadata/{metadataId}", new
                            {
                                isMasked = false,
                                isProfilerWritable = false
                            });
                            fixedCount++;
                        }

                        if (fixedCount > 0)
                            await WriteStatus($"[{tableLabel}] Fixed {fixedCount} column rule(s).");
                    }
                    catch (Exception ex)
                    {
                        await WriteStatus($"[{tableLabel}] Warning: Could not apply mapping rules: {ex.Message}");
                    }
                }

                // Run masking job
                await WriteStatus("Running masking job...");
                using var maskingExecResp = await EngineRelayService.RelayHttpAsync(connection, engineBaseUrl, dataEngineConfig.AuthorizationToken, "POST", "executions", new
                {
                    jobId = int.Parse(g2MaskingJobId)
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

                // Copy masked files back per table
                await WriteStatus("Copying masked results...");
                var perTableComplete = new List<object>();
                foreach (var tr in tableResults)
                {
                    var maskedFilenames = new List<string>();
                    foreach (var previewFile in tr.Filenames)
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

                    perTableComplete.Add(new
                    {
                        rowKey = tr.RowKey,
                        schema = tr.Schema,
                        tableName = tr.TableName,
                        fileFormatId = tr.FileFormatId,
                        maskedFilenames,
                        sqlColumnTypes = tr.SqlColumnTypes,
                    });
                }

                var evtSummary = $"DP preview (multi) completed: {tables.Count} table(s), " +
                    $"fileRulesetId={g2RulesetId}, profileJobId={g2ProfileJobId} ({profileStatus}), " +
                    $"maskingJobId={g2MaskingJobId} ({maskingStatus})";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview_multi", evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_preview_multi", summary = evtSummary, detail = "" }));

                var completeJson = JsonSerializer.Serialize(new
                {
                    success = true,
                    fileRulesetId = g2RulesetId,
                    profileJobId = g2ProfileJobId,
                    profileStatus,
                    maskingJobId = g2MaskingJobId,
                    maskingStatus,
                    tables = perTableComplete,
                });
                await SseWriter.WriteEventAsync(response, "complete", completeJson);
            }
            catch (TimeoutException)
            {
                var evtSummary = "DP preview (multi): timeout";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview_multi", evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_preview_multi", summary = evtSummary, detail = "" }));
                await SseWriter.WriteErrorAsync(response, "Agent did not respond within the timeout period.");
            }
            catch (Exception ex)
            {
                var errMsg = ex is AggregateException agg ? agg.InnerExceptions[0].Message : ex.Message;
                var evtSummary = $"DP preview (multi) error: {errMsg}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview_multi", evtSummary, stepsDetail);
                await SseWriter.WriteEventAsync(response, "event", JsonSerializer.Serialize(new { timestamp = DateTime.UtcNow.ToString("O"), type = "dp_preview_multi", summary = evtSummary, detail = "" }));
                await SseWriter.WriteErrorAsync(response, $"DP preview (multi) error: {errMsg}");
            }
        });

        app.MapPost("/api/agents/{path}/dp-run", async (string path, HttpContext httpContext, AgentRegistry registry,
            ClientTableService clientTableService, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            if (!registry.TryGetConnection(path, out var connection) || connection is null)
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

                // Export full table via agent
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

                // Create file ruleset
                await WriteStatus("Creating file ruleset...");
                var fullRunUuid = Guid.NewGuid().ToString("N");

                var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(
                    $"fullrun_ruleset_{fullRunUuid}", dataEngineConfig.ConnectorId);
                if (!rulesetSuccess)
                {
                    await SseWriter.WriteErrorAsync(response, "File ruleset creation failed.");
                    return;
                }

                // Create file metadata for each exported file
                var (metaBatchSuccess, fileMetadataIds) = await EngineRelayService.CreateFileMetadataBatchAsync(
                    engineApi, response, exportFilenames, fileRulesetId, fileFormatId, statusSteps);
                if (!metaBatchSuccess) return;

                // Create masking job
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

                // Execute masking job
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

                // Poll masking job status
                await WriteStatus("Waiting for masking to complete...");
                var maskingStatus = await EngineRelayService.PollExecutionAsync(
                    connection, engineBaseUrl, dataEngineConfig.AuthorizationToken,
                    maskingExecId, response, "", maxIterations: 600, statusSteps: statusSteps);

                if (maskingStatus is not ("SUCCEEDED" or "WARNING"))
                {
                    await SseWriter.WriteErrorAsync(response, $"Masking job did not succeed. Final status: {maskingStatus}");
                    return;
                }

                // Load masked data to destination table
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

        app.MapGet("/api/agents/{path}/column-rules", async (string path, string? fileFormatId, AgentRegistry registry, EngineApiClient engineApi, EngineMetadataService metadataService) =>
        {
            if (!registry.TryGet(path, out _))
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

        app.MapPut("/api/agents/{path}/column-rule/{fileFieldMetadataId}", async (string path, string fileFieldMetadataId, HttpRequest request, AgentRegistry registry, DataEngineConfig dataEngineConfig) =>
        {
            if (!registry.TryGetConnection(path, out var connection) || connection is null)
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

        app.MapGet("/api/agents/{path}/engine-metadata", async (string path, AgentRegistry registry, EngineMetadataService metadataService) =>
        {
            if (!registry.TryGet(path, out _))
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

    private class TablePrepResult
    {
        public string RowKey { get; set; } = "";
        public string Schema { get; set; } = "";
        public string TableName { get; set; } = "";
        public List<string> Filenames { get; set; } = new();
        public string FileFormatId { get; set; } = "";
        public List<string> PreviewHeaders { get; set; } = new();
        public List<string> SqlColumnTypes { get; set; } = new();
        public List<string> FileMetadataIds { get; set; } = new();
    }
}
