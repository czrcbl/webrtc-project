using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Rtc = LiveKit.Rtc;

public class SessionRegistry(SessionFactory sessionFactory, ILogger<SessionRegistry> logger)
{
    private readonly ConcurrentDictionary<string, SessionManager> _sessions = new();

    public string[] GetRoomIds()
    {
        return _sessions.Keys.ToArray<string>();
    }

    public async Task LeaveIfRoomEmpty(string roomName)
    {
        logger.LogInformation("Room {RoomName}: participant left, checking if empty", roomName);
        var isEmpty = await _sessions[roomName].DisconnectIfNoRemoteParticipant();
        if (isEmpty)
        {
            _sessions[roomName].CancellationSource.Cancel();
        }
    }

    public async Task StartSessionFromHookAsync(string roomName)
    {
        logger.LogInformation("Starting session from hook for room {RoomName}", roomName);
        CreateSession(roomName);
    }

    public void CreateSession(string roomName)
    {
        if (!_sessions.ContainsKey(roomName))
        {
            Console.WriteLine($"Starting Session for Room {roomName}");
            var session = sessionFactory.Create(roomName);
            var _ = session.RunAsync();
            _sessions[roomName] = session;
        }
    }

    public async Task CancelSessionFromHookAsync(string roomName)
    {
        logger.LogInformation("Cancelling session from hook for room {RoomName}", roomName);
        CancelSession(roomName);
    }

    public bool CancelSession(string roomId)
    {
        if (_sessions.ContainsKey(roomId))
        {
            _sessions[roomId].CancellationSource.Cancel();
            return _sessions.TryRemove(roomId, out _);
        }
        return false;
    }
}

public class SessionFactory(
    // LiveKitSdkUtils utils,
    ILogger<SessionFactory> logger,
    IServiceProvider sp
)
{
    public SessionManager Create(string roomId)
    {
        // Hadle DI into non registered types
        return ActivatorUtilities.CreateInstance<SessionManager>(sp, roomId);
    }
}

/// <summary>
/// Manages the connection with a LiveKit server Room
/// Watches for existing and new published audio tracks
/// </summary>
/// <param name="roomId"></param>
public class SessionManager(string roomId, LiveKitSdkUtils utils, ILogger<SessionManager> logger)
{
    private readonly string _roomId = roomId;
    public Task? WorkerTask;

    private readonly Rtc.Room _roomRtc = new();

    private readonly List<Task> _executorTasks = new();
    public CancellationTokenSource CancellationSource = new();

    private static bool IsAudioPub(Rtc.TrackPublication publication)
    {
        return publication.Kind == LiveKit.Proto.TrackKind.KindAudio;
    }

    private bool SubscribeIfAudio(Rtc.RemoteTrackPublication publication)
    {
        logger.LogInformation("Track kind {TrackKind}", publication.Kind);
        if (IsAudioPub(publication))
        {
            publication.SetSubscribed(true);
            logger.LogInformation("Track subscribed {Sid}", publication.Sid);
        }
        return true;
    }

    public async Task<bool> DisconnectIfNoRemoteParticipant()
    {
        if (_roomRtc.RemoteParticipants.Count == 0)
        {
            await _roomRtc.DisconnectAsync();
            return true;
        }
        return false;
    }

    private void RegisterCallbacks(Rtc.Room roomRtc)
    {
        // Callbacks

        // Hidden Agent keeps the room from closing
        // Triggers on the hook callback
        // roomRtc.ParticipantDisconnected += async (_, _) =>
        // {
        //     if (roomRtc.RemoteParticipants.Count == 0)
        //         await roomRtc.DisconnectAsync();
        // };

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
            var exec = new Executor(roomRtc, e.Participant, e.Track, CancellationSource.Token);
            var execTask = exec.RunAsync();
            Console.WriteLine($"Starting executor for track {e.Track.Sid}");
            _executorTasks.Add(execTask);
        };
    }

    private void ScanAndSubscribe()
    {
        foreach (var (pId, participant) in _roomRtc.RemoteParticipants)
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

        logger.LogInformation("Starting worker for room {RoomId}", _roomId);

        var token = utils.MintAgentToken(_roomId);

        await _roomRtc.ConnectAsync(
            Configs.liveKitUrl,
            token,
            new Rtc.RoomOptions { AutoSubscribe = false } // Check wether it's audio
        );

        logger.LogInformation("Room {RoomId} connected", _roomId);

        RegisterCallbacks(_roomRtc);

        ScanAndSubscribe();

        try
        {
            while (!CancellationSource.Token.IsCancellationRequested)
                await Task.WhenAny(_executorTasks);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Exception {e}");
        }
    }
}
