/* Application lifecycle, check latter
Microsoft.Extensions.Hosting
Also, ASP.net equivalent.
*/

public static class Program
{
    // public static async Task Main(string[] args)
    // {
    // Console.WriteLine("Starting Program");

    // string roomId = args.Length == 1 ? args[0] : await SessionManager.GetSingleRoomAsync();
    // var manager = new SessionManager(roomId);
    // await manager.RunAsync();
    // await Watcher.RunAsync();
    // }

    private static async Task<string> HookCallBack()
    {
        Console.WriteLine("Api Reached");
        return "Hello, World!";
    }

    public static async Task Main(string[] args)
    {

        var builder = WebApplication.CreateBuilder(args);

        builder.WebHost.UseUrls("http://localhost:5000");

        builder.Services.AddSingleton<Watcher>();
        builder.Services.AddHostedService<WatcherService>();

        var app = builder.Build();

        if (app.Environment.IsDevelopment())
        {
            Console.WriteLine(">>> Running on Development <<<");
        }
        else
        {
            Console.WriteLine(">>> Running on Production <<<");
            app.UseExceptionHandler("/Error");
            // Enforce HTTPS, add header
            app.UseHsts();
            app.UseHttpsRedirection();
        }

        app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);

        app.MapGet("/v1/livekit/room-hook",
            HookCallBack);

        app.MapGet("/v1/livekit/sessions",
            (Watcher w) => w.GetRoomIds());

        app.Run();
    }

}

