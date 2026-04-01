using System.Text.Json;

namespace DataProtectionTool.ControlCenter.HttpServer.Services;

public static class SseWriter
{
    public static void SetupHeaders(HttpResponse response)
    {
        response.ContentType = "text/event-stream";
        response.Headers["Cache-Control"] = "no-cache";
        response.Headers["Connection"] = "keep-alive";
    }

    public static async Task WriteEventAsync(HttpResponse response, string eventType, string data)
    {
        await response.WriteAsync($"event: {eventType}\ndata: {data}\n\n");
        await response.Body.FlushAsync();
    }

    public static async Task WriteErrorAsync(HttpResponse response, string message)
    {
        var json = JsonSerializer.Serialize(new { success = false, message });
        await WriteEventAsync(response, "error", json);
    }
}
