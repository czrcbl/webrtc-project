using System.ComponentModel.DataAnnotations;
using System.Net;
using Microsoft.Extensions.Options;

public static class Configs
// TODO: could be done if a record type?
{
    public static string liveKitApiKey { get; set; } = GetEnvRequired("LIVEKIT_API_KEY");
    public static string liveKitApiSecret { get; set; } = GetEnvRequired("LIVEKIT_API_SECRET");

    public static string liveKitUrl = GetEnvRequired("LIVEKIT_URL");
    public static string listenerIdentity = "listener";
    public static string participantName = "Listener";

    public static uint sampleRate = 16_000;
    public static uint numChannels = 1;

    public static bool WebHookEnabled = true;

    static string GetEnvRequired(string varName)
    {
        return Environment.GetEnvironmentVariable(varName)
            ?? throw new InvalidOperationException($"Required var not set {varName}");
    }
}

public class LiveKitOptions
{
    [ConfigurationKeyName("LIVEKIT_API_KEY"), Required]
    public required string LiveKitApiKey { get; init; }

    [ConfigurationKeyName("LIVEKIT_API_SECRET"), Required]
    public required string LiveKitApiSecret { get; init; }

    [ConfigurationKeyName("LIVEKIT_URL"), Required]
    public required string LiveKitUrl { get; init; }
}
