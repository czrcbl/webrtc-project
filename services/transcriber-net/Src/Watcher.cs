using System.Collections.Concurrent;

/// <summary>
/// Watches for new rooms on the livekit server.
/// Spawn a session manager for each room
/// </summary>
// TODO: Move the Watcher logic inside it?
public class WatcherService(Watcher watcher) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return watcher.RunAsync(stoppingToken);
    }
}

public class Watcher
{
    private HashSet<string> _registeredRooms = new HashSet<string>();

    // TODO: review ConcurrentDictionary need
    private readonly ConcurrentDictionary<string, SessionManager> _sessions =
        new ConcurrentDictionary<string, SessionManager>();

    public string[] GetRoomIds()
    {
        return _registeredRooms.ToArray();
    }

    public async Task StartSessionFromHookAsync(string roomName)
    {
        CreateSession(roomName);
    }

    public void CreateSession(string roomName)
    {
        if (!_sessions.ContainsKey(roomName))
        {
            Console.WriteLine($"Starting Session for Room {roomName}");
            var session = new SessionManager(roomName);
            var _ = session.RunAsync();
            _sessions[roomName] = session;
        }
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

    public async Task RoomWatcherAsync(CancellationToken cancellationToken)
    {
        // Non hook watch. Still useful for when the Transcriber is started and Rooms already exist
        while (!cancellationToken.IsCancellationRequested)
        {
            var roomNames = await LiveKitUtils.GetRoomsAsync();
            var currentRooms = roomNames.ToHashSet();

            var newRooms = currentRooms.Except(_registeredRooms).ToHashSet();
            var goneRooms = _registeredRooms.Except(currentRooms).ToHashSet();

            if (goneRooms.Count > 0)
                Console.WriteLine($"Rooms removed: {string.Join(", ", goneRooms)}");

            Console.WriteLine($"Current Rooms: {string.Join(", ", currentRooms)}");

            if (!Configs.WeebHookEnabled)
            {
                foreach (var r in newRooms)
                {
                    CreateSession(r);
                }
            }
            foreach (var r in goneRooms)
            {
                if (CancelSession(r))
                    Console.WriteLine($"Session {r} cancelled.");
                else
                    Console.WriteLine($"Unable to cancel {r}.");
            }
            _registeredRooms = currentRooms;
            await Task.Delay(2000, cancellationToken);
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var watcher = RoomWatcherAsync(cancellationToken);
        await watcher;
    }
}
