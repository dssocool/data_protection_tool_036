namespace DataProtectionTool.ControlCenter.Models;

public class BlobStorageConfig
{
    public const string PreviewContainer = "data_preview";

    public string StorageAccount { get; set; } = "";
    public string Container { get; set; } = "";
    public string AccessKey { get; set; } = "";
}
