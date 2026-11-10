public static class Configs
{
    public static string LIVEKIT_API_KEY { get; set; } = GetEnvRequired("LIVEKIT_API_KEY");
    public static string LIVEKIT_API_SECRET { get; set; } = GetEnvRequired("LIVEKIT_API_SECRET");
    public static string listenerIdentity = "listener";
    public static string participantName = "Listener";

    static string GetEnvRequired(string varName)
    {
        return Environment.GetEnvironmentVariable(varName)
            ?? throw new InvalidOperationException($"Required var not set {varName}");
    }
}