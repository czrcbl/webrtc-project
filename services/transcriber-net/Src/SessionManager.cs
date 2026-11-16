using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Rtc = LiveKit.Rtc;

public class SessionRegistry(SessionFactory sessionFactory, ILogger<SessionRegistry> logger)
{
    private readonly ConcurrentDictionary<string, SessionManager> _sessions =
        new ConcurrentDictionary<string, SessionManager>();

    public string[] GetRoomIds()
    {
        return _sessions.Keys.ToArray<string>();
    }

    public async Task LeaveIfRoomEmpty(string roomName)
    {
        logger.LogInformation($"room: {roomName}, participant left check");
        var isEmpty = await _sessions[roomName].DisconnectIfNoRemoteParticipant();
        if (isEmpty)
        {
            _sessions[roomName].CancellationSource.Cancel();
        }
    }

    public async Task StartSessionFromHookAsync(string roomName)
    {
        logger.LogInformation($"Watcher: Starting room from hook {roomName}");
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
        logger.LogInformation($"Watcher: Starting room from hook {roomName}");
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
    LiveKitSdkUtils utils,
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

    private Rtc.Room _roomRtc;

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
            var exec = new Executor(roomRtc, e.Participant, e.Track, CancellationSource.Token);
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

        logger.LogInformation($"Room ID: {_roomId}");

        _roomRtc = new Rtc.Room();
        var token = utils.MintAgentToken(_roomId);
        await _roomRtc.ConnectAsync(
            Configs.liveKitUrl,
            token,
            new Rtc.RoomOptions { AutoSubscribe = false } // Check wether it's audio
        );

        Console.WriteLine("Room connected");

        RegisterCallbacks(_roomRtc);

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

        try
        {
            while (!CancellationSource.Token.IsCancellationRequested)
                await Task.WhenAny(_executorTasks);
        }
        // TODO: Define errors
        catch (Exception e)
        {
            Console.WriteLine($"Exception {e}");
        }
    }
}
