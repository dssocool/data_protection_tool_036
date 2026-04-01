using System.Text.Json;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Parquet;
using DataProtectionTool.HttpServer.Helpers;
using DataProtectionTool.HttpServer.Models;
using DataProtectionTool.HttpServer.Services;

namespace DataProtectionTool.HttpServer.Endpoints;

public static class BlobEndpoints
{
    public static void MapBlobEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/blob/{filename}", async (string filename, BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            if (!EndpointHelpers.IsValidPreviewFilename(filename))
                return Results.BadRequest(new { error = "Invalid filename." });

            try
            {
                var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
                var blob = containerClient.GetBlobClient(filename);

                if (!await blob.ExistsAsync())
                    return Results.NotFound(new { error = "Blob not found." });

                var download = await blob.DownloadContentAsync();
                using var ms = new MemoryStream(download.Value.Content.ToArray());
                using var reader = await ParquetReader.CreateAsync(ms);

                var dataFields = reader.Schema.GetDataFields();
                var headers = dataFields.Select(f => f.Name).ToList();
                string[]? columnTypes = null;
                if (reader.CustomMetadata != null
                    && reader.CustomMetadata.TryGetValue("sql_types", out var sqlTypesJson))
                {
                    columnTypes = JsonSerializer.Deserialize<string[]>(sqlTypesJson);
                }
                var rows = await ParquetReaderService.ReadRowsAsync(reader);

                return Results.Json(new { headers, rows, columnTypes });
            }
            catch (Exception ex)
            {
                return Results.Problem($"Failed to read blob: {ex.Message}");
            }
        });

        app.MapPost("/api/blob/preview-merge", async (HttpRequest request, BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                if (!doc.RootElement.TryGetProperty("filenames", out var filenamesEl)
                    || filenamesEl.ValueKind != JsonValueKind.Array
                    || filenamesEl.GetArrayLength() == 0)
                {
                    return Results.BadRequest(new { error = "filenames array is required." });
                }

                var filenames = filenamesEl.EnumerateArray().Select(e => e.GetString() ?? "").ToList();
                if (filenames.Any(f => !EndpointHelpers.IsValidPreviewFilename(f)))
                    return Results.BadRequest(new { error = "Invalid filename in list." });

                var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
                List<string>? headers = null;
                string[]? columnTypes = null;
                var rows = new List<List<string?>>();

                foreach (var filename in filenames)
                {
                    var blob = containerClient.GetBlobClient(filename);
                    if (!await blob.ExistsAsync())
                        return Results.NotFound(new { error = $"Blob not found: {filename}" });

                    var download = await blob.DownloadContentAsync();
                    using var ms = new MemoryStream(download.Value.Content.ToArray());
                    using var reader = await ParquetReader.CreateAsync(ms);

                    var dataFields = reader.Schema.GetDataFields();
                    var fileHeaders = dataFields.Select(f => f.Name).ToList();

                    if (headers == null)
                    {
                        headers = fileHeaders;
                        if (reader.CustomMetadata != null
                            && reader.CustomMetadata.TryGetValue("sql_types", out var sqlTypesJson))
                        {
                            columnTypes = JsonSerializer.Deserialize<string[]>(sqlTypesJson);
                        }
                    }
                    else if (!headers.SequenceEqual(fileHeaders))
                    {
                        return Results.BadRequest(new { error = $"Schema mismatch in {filename}." });
                    }

                    rows.AddRange(await ParquetReaderService.ReadRowsAsync(reader));
                }

                return Results.Json(new { headers = headers ?? new List<string>(), rows, columnTypes });
            }
            catch (Exception ex)
            {
                return Results.Problem($"Failed to merge blobs: {ex.Message}");
            }
        });

        app.MapGet("/api/agents/{path}/sas-token", async (
            string path,
            string? containerName,
            RpcAgentProxy agentProxy,
            BlobStorageConfig blobStorageConfig,
            StorageSharedKeyCredential? blobCredential) =>
        {
            if (!await agentProxy.TryGetAsync(path))
                return Results.NotFound(new { success = false, error = "Agent not found." });

            if (blobCredential == null)
                return Results.Ok(new { success = false, error = "Blob storage credentials are not configured." });

            try
            {
                var sasBuilder = new AccountSasBuilder
                {
                    Services = AccountSasServices.Blobs,
                    ResourceTypes = AccountSasResourceTypes.Object,
                    ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15),
                    Protocol = SasProtocol.HttpsAndHttp
                };
                sasBuilder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write | AccountSasPermissions.Create);

                var sasToken = sasBuilder.ToSasQueryParameters(blobCredential).ToString();

                var blobEndpoint = blobStorageConfig.StorageAccount == "devstoreaccount1"
                    ? $"http://127.0.0.1:10000/{blobStorageConfig.StorageAccount}"
                    : $"https://{blobStorageConfig.StorageAccount}.blob.core.windows.net";

                var container = string.IsNullOrEmpty(containerName)
                    ? blobStorageConfig.PreviewContainer
                    : containerName;

                return Results.Ok(new
                {
                    success = true,
                    sasToken,
                    blobEndpoint,
                    container
                });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, error = ex.Message });
            }
        });

        app.MapPost("/api/blob/delete-preview", async (HttpRequest request, BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                if (!doc.RootElement.TryGetProperty("filenames", out var filenamesEl)
                    || filenamesEl.ValueKind != JsonValueKind.Array)
                {
                    return Results.BadRequest(new { error = "filenames array is required." });
                }

                var filenames = filenamesEl.EnumerateArray().Select(e => e.GetString() ?? "").ToList();
                if (filenames.Any(f => !EndpointHelpers.IsValidPreviewFilename(f)))
                    return Results.BadRequest(new { error = "Invalid filename in list." });

                var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
                int deleted = 0;
                foreach (var filename in filenames)
                {
                    var blob = containerClient.GetBlobClient(filename);
                    if (await blob.DeleteIfExistsAsync())
                        deleted++;
                }

                return Results.Ok(new { success = true, deleted });
            }
            catch (Exception ex)
            {
                return Results.Problem($"Failed to delete blobs: {ex.Message}");
            }
        });
    }
}
