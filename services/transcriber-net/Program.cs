





/* Application lifecycle, check latter
Microsoft.Extensions.Hosting
Also, ASP.net equivalent.
*/
Console.WriteLine("Hello, World!");

string roomId = args.Length == 1 ? args[0] : throw new ArgumentException("Missing room ID");
var manager = new SessionManager(roomId);

await manager.RunAsync();