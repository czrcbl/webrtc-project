/* Application lifecycle, check latter
Microsoft.Extensions.Hosting
Also, ASP.net equivalent.
*/
public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Program");

        // string roomId = args.Length == 1 ? args[0] : await SessionManager.GetSingleRoomAsync();
        // var manager = new SessionManager(roomId);
        // await manager.RunAsync();
        await Watcher.RunAsync();
    }
}

