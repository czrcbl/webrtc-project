/* Application lifecycle, check latter
Microsoft.Extensions.Hosting
Also, ASP.net equivalent.
*/
Console.WriteLine("Starting Main");

string roomId = args.Length == 1 ? args[0] : await SessionManager.GetSingleRoomAsync();
var manager = new SessionManager(roomId);

await manager.RunAsync();