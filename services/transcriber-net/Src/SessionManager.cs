using Livekit.Server.Sdk.Dotnet;

using LiveKit.Rtc;

public class SessionManager(string roomId)
{

    // TODO: get the id with a webhook
    private string _roomId = roomId;

    public static string GetUserToken(string roomName)
    {

        var token = new AccessToken(Configs.LIVEKIT_API_KEY, Configs.LIVEKIT_API_SECRET)
        .WithIdentity(Configs.listenerIdentity)
        .WithName(Configs.participantName)
        .WithGrants(new VideoGrants
        {
            RoomJoin = true,
            Room = roomName,
            CanSubscribe = true,
            CanPublish = false,
            CanPublishData = true,
            Hidden = true,
        });
        // .WithAttributes(new Dictionary<string, string> { { "mykey", "myvalue" } })
        // .WithTtl(TimeSpan.FromHours(1));

        var jwt = token.ToJwt();
        return jwt;
    }

    public async Task RunAsync()
    {
        while (true)
        {
            try
            {
                Console.WriteLine("I am Running!!!");
                Console.WriteLine($"Room ID: {_roomId}");
                await Task.Delay(2000);
            }
            // TODO: Define errors
            catch (Exception e)
            {
                Console.WriteLine($"Exception {e}");
            }
        }
    }
}