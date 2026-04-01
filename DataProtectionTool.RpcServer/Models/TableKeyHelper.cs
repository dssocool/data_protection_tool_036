using System.Text;

namespace DataProtectionTool.ControlCenter.RpcServer.Models;

public static class TableKeyHelper
{
    public static string EscapeKeySegment(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        var sb = new StringBuilder(value.Length);
        foreach (var c in value)
        {
            if (c == '\\' || c == '/' || c == '#' || c == '?' || char.IsControl(c))
                sb.Append($"_0x{(int)c:X2}_");
            else
                sb.Append(c);
        }

        return sb.ToString();
    }
}
