using Microsoft.Extensions.Options;
using Sdk = Livekit.Server.Sdk.Dotnet;

/// <summary>
/// Collection of LiveKit related utilities
/// </summary>
public class LiveKitSdkUtils(IOptions<LiveKitOptions> op)
{
    private readonly Sdk.RoomServiceClient _client = new(
        op.Value.LiveKitUrl,
        op.Value.LiveKitApiKey,
        op.Value.LiveKitApiSecret
    );

    public string MintAgentToken(string roomName)
    {
        var token = new Sdk.AccessToken(Configs.liveKitApiKey, Configs.liveKitApiSecret)
            .WithIdentity(Configs.listenerIdentity)
            .WithName(Configs.participantName)
            .WithGrants(
                new Sdk.VideoGrants
                {
                    RoomJoin = true,
                    Room = roomName,
                    CanSubscribe = true,
                    CanPublish = false,
                    CanPublishData = true,
                    Hidden = true,
                }
            );
        // .WithAttributes(new Dictionary<string, string> { { "mykey", "myvalue" } })
        // .WithTtl(TimeSpan.FromHours(1));

        var jwt = token.ToJwt();
        return jwt;
    }

    public async Task<string[]> GetRoomsAsync()
    {
        // List rooms
        var listRoomsRequest = new Sdk.ListRoomsRequest();
        var listRoomsResponse = await _client.ListRooms(listRoomsRequest);
        Sdk.Room[] rooms = listRoomsResponse.Rooms.ToArray();
        return Array.ConvertAll(rooms, r => r.Name);
    }

    public async Task<string> GetSingleRoomAsync()
    {
        var roomNames = await GetRoomsAsync();
        return roomNames.Length == 1
            ? roomNames[0]
            : throw new InvalidOperationException(
                $"Exactly one room is required to exist on the LiveKit server"
            );
    }
}
