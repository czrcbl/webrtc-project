using Rtc = LiveKit.Rtc;

/// <summary>
/// Manages the connection with a LiveKit server Room
/// Watches for existing and new published audio tracks
/// </summary>
/// <param name="roomId"></param>
public class SessionManager(string roomId)
{
    // TODO: get the id with a webhook
    // TODO: check the 'lifecycle' of the input var Ref: CS9124
    private readonly string _roomId = roomId;
    public Task? WorkerTask;

    private readonly List<Task> _executorTasks = new List<Task>();
    public CancellationTokenSource CancellationSource = new CancellationTokenSource();

    private static bool IsAudioPub(Rtc.TrackPublication publication)
    {
        if (publication.Kind == LiveKit.Proto.TrackKind.KindAudio)
        {
            return true;
        }
        return false;
    }

    private static bool SubscribeIfAudio(Rtc.RemoteTrackPublication publication)
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

    private void RegisterCallbacks(Rtc.Room roomRtc)
    {
        // Callbacks

        // Hidden Agent keeps the room from closing
        roomRtc.ParticipantDisconnected += async (_, _) =>
        {
            if (roomRtc.RemoteParticipants.Count == 0)
                await roomRtc.DisconnectAsync();
        };

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
            // TODO: check cancellation token need
            var exec = new Executor(roomRtc, e.Participant, e.Track);
            var execTask = exec.RunAsync();
            Console.WriteLine($"Starting executor for track {e.Track.Sid}");
            _executorTasks.Add(execTask);
        };
    }

    public async Task<Task> RunAsync()
    {
        // Wrapper to keep a reference the task on the manager
        WorkerTask = RunWorkerAsync(CancellationSource.Token);
        return WorkerTask;
    }

    private async Task RunWorkerAsync(CancellationToken ct)
    {
        // TODO: Recovery. Restart executor on error if track still exists;

        Console.WriteLine($"Room ID: {_roomId}");

        var roomRtc = new Rtc.Room();
        var token = LiveKitUtils.MintAgentToken(_roomId);
        await roomRtc.ConnectAsync(
            Configs.liveKitUrl,
            token,
            new Rtc.RoomOptions { AutoSubscribe = false }  // Check wether it's audio
        );

        Console.WriteLine("Room connected");

        RegisterCallbacks(roomRtc);

        foreach (var (pId, participant) in roomRtc.RemoteParticipants)
        {
            Console.WriteLine($"Participant {pId}");
            foreach (var (tId, trackPub) in participant.TrackPublications)
            {
                // Check this pattern
                if (trackPub is Rtc.RemoteTrackPublication remote)
                {

                    Console.WriteLine("Remote Track found");
                    SubscribeIfAudio(remote);
                }
            }
        }

        try
        {
            while (_executorTasks.Count > 0 && !CancellationSource.Token.IsCancellationRequested)
                await Task.WhenAll(_executorTasks);
        }
        // TODO: Define errors
        catch (Exception e)
        {
            Console.WriteLine($"Exception {e}");
        }
    }
}