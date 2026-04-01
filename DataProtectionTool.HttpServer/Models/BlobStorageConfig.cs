namespace DataProtectionTool.ControlCenter.HttpServer.Models;

public class BlobStorageConfig
{
    public string StorageAccount { get; set; } = "";
    public string Container { get; set; } = "";
    public string AccessKey { get; set; } = "";
    public string PreviewContainer { get; set; } = "";
}
