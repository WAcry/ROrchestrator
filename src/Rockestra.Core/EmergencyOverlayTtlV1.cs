using System.Text.Json;

namespace Rockestra.Core;

internal static class EmergencyOverlayTtlV1
{
    public const string TtlExpiredCode = "EMERGENCY_TTL_EXPIRED";

    public static bool IsExpired(JsonElement emergencyPatch, DateTimeOffset configTimestampUtc, long nowUtcTicks)
    {
        if (emergencyPatch.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (!emergencyPatch.TryGetProperty("ttl_minutes", out var ttlMinutesElement)
            || ttlMinutesElement.ValueKind != JsonValueKind.Number
            || !ttlMinutesElement.TryGetInt32(out var ttlMinutes)
            || ttlMinutes <= 0)
        {
            return false;
        }

        var ttlTicks = (long)ttlMinutes * TimeSpan.TicksPerMinute;
        var expiryUtcTicks = configTimestampUtc.UtcTicks + ttlTicks;
        return expiryUtcTicks <= nowUtcTicks;
    }
}

