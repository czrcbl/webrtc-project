using System.Net.Quic;

using Livekit.Server.Sdk.Dotnet;

using LiveKit.Rtc;
// TODO: can names be changed? 

public class SessionManager(string roomId)
{

    // TODO: get the id with a webhook
    // TODO: check the 'lifecycle' of the input var Ref: CS9124
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

    public static async Task<string> GetSingleRoomAsync()
    {
        RoomServiceClient client = new RoomServiceClient(
        Configs.liveKitUrl,
        Configs.LIVEKIT_API_KEY,
        Configs.LIVEKIT_API_SECRET
        );

        // List rooms
        var listRoomsRequest = new ListRoomsRequest();
        var listRoomsResponse = await client.ListRooms(listRoomsRequest);
        Livekit.Server.Sdk.Dotnet.Room[] rooms = listRoomsResponse.Rooms.ToArray();
        return rooms.Length == 1 ? rooms[0].Name : throw new InvalidOperationException($"Exactly one room is required to exist on the LiveKit server");

    }

    private static bool IsAudioPub(TrackPublication publication)
    {
        if (publication.Kind == LiveKit.Proto.TrackKind.KindAudio)
        {
            return true;
        }
        return false;
    }

    private static bool SubscribeIfAudio(RemoteTrackPublication publication)
    // TODO: Check why Track is null on the publication object
    {
        Console.WriteLine($"Track kind {publication.Kind}");
        if (IsAudioPub(publication))
        {
            publication.SetSubscribed(true);
            Console.WriteLine($"Track subscribed {publication.Sid}");
        }
        return true;
    }

    public async Task RunAsync()
    {

        Console.WriteLine($"Room ID: {_roomId}");

        List<Task> executorTasks = new List<Task>();

        var roomRtc = new LiveKit.Rtc.Room();
        var token = GetUserToken(_roomId);
        await roomRtc.ConnectAsync(
            Configs.liveKitUrl,
            token,
            // TODO: constructor x Properties | () x {}
            new RoomOptions { AutoSubscribe = false }  // Check wether it's audio
        );

        Console.WriteLine("Room connected");

        foreach (var (pId, participant) in roomRtc.RemoteParticipants)
        {
            Console.WriteLine($"Participant {pId}");
            foreach (var (tId, trackPub) in participant.TrackPublications)
            {
                // Check this pattern
                if (trackPub is RemoteTrackPublication remote)
                {

                    Console.WriteLine("Remote Track found");
                    SubscribeIfAudio(remote);
                }
            }
        }

        // Callbacks

        // Add callback pattern
        roomRtc.TrackPublished += (sender, e) =>

        {
            if (IsAudioPub(e.Publication))
            {
                Console.WriteLine("Audio found!");
            }
            SubscribeIfAudio(e.Publication);
        };

        roomRtc.TrackSubscribed += (sender, e) =>
        {
            var exec = new Executor(roomRtc, e.Participant, e.Track);
            var execTask = exec.RunAsync();
            Console.WriteLine($"Starting executor for track {e.Track.Sid}");
            executorTasks.Add(execTask);
        };

        await Task.WhenAll(executorTasks);




        while (true)
        {
            try
            {
                Console.WriteLine("I am Running!!!");
                await Task.Delay(5000);
            }
            // TODO: Define errors
            catch (Exception e)
            {
                Console.WriteLine($"Exception {e}");
            }
        }
    }
}