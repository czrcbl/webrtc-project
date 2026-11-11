using System.Collections.Concurrent;

/// <summary>
/// Watches for new rooms on the livekit server.
/// Spawn a session manager for each room
/// </summary>

public static class Watcher
{
    // Add a proper Webhook with ASP net later

    private static HashSet<string> _registeredRooms = new HashSet<string>();
    // TODO: review ConcurrentDictionary need
    private static readonly ConcurrentDictionary<string, SessionManager> _sessions = new ConcurrentDictionary<string, SessionManager>();




    public static async Task RoomWatcherAsync()
    {

        // Non hook watch. Still useful for when the Transcriber is started and Rooms already exist
        while (true)
        {
            var roomNames = await LiveKitUtils.GetRoomsAsync();
            var currentRooms = roomNames.ToHashSet();

            var newRooms = currentRooms.Except(_registeredRooms).ToHashSet();
            var goneRooms = _registeredRooms.Except(currentRooms).ToHashSet();

            if (goneRooms.Count > 0)
                Console.WriteLine($"Rooms removed: {string.Join(", ", goneRooms)}");

            Console.WriteLine($"Current Rooms: {string.Join(", ", currentRooms)}");

            foreach (var r in newRooms)
            {
                Console.WriteLine($"Starting Session for Room {r}");
                var session = new SessionManager(r);
                var t = session.RunAsync();
                _sessions[r] = session;
            }

            foreach (var r in goneRooms)
            {
                if (_sessions.ContainsKey(r))
                {
                    _sessions[r].CancellationSource.Cancel();
                    _sessions.TryRemove(r, out _);
                }
            }

            _registeredRooms = currentRooms;
            await Task.Delay(2000);
        }
    }

    public static async Task RunAsync()
    {
        var watcher = RoomWatcherAsync();
        await watcher;
    }
}