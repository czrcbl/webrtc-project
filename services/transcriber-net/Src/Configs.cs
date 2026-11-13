using System.Net;

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

    public static bool WeebHookEnabled = true;

    static string GetEnvRequired(string varName)
    {
        return Environment.GetEnvironmentVariable(varName)
            ?? throw new InvalidOperationException($"Required var not set {varName}");
    }
}
