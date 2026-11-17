using Livekit.Server.Sdk.Dotnet;
using Microsoft.Extensions.Options;

/* Application lifecycle, check latter
Microsoft.Extensions.Hosting
Also, ASP.net equivalent.
*/

public static class Program
{
    static readonly WebhookReceiver _receiver = new(
        Configs.liveKitApiKey,
        Configs.liveKitApiSecret
    );

    private static async Task WebhookCallback(
        HttpRequest request,
        SessionRegistry sessionReg,
        ILoggerFactory loggerFactory
    )
    {
        var logger = loggerFactory.CreateLogger("LiveKitWebhook");

        using var body = new MemoryStream();
        await request.Body.CopyToAsync(body);

        WebhookEvent webhookEvent = _receiver.Receive(
            body.ToArray(),
            request.Headers.Authorization.ToString()
        );

        if (webhookEvent.Room is null)
        {
            // TODO: handle properly
            logger.LogInformation($"Room name is null");
            return;
        }
        var roomName = webhookEvent.Room.Name;

        logger.LogInformation($"{webhookEvent.Event} in room {webhookEvent.Room?.Name}");
        if (webhookEvent.Event == "room_started")
        {
            await sessionReg.StartSessionFromHookAsync(roomName);
        }
        else if (webhookEvent.Event == "participant_left")
        {
            await sessionReg.LeaveIfRoomEmpty(roomName);
        }
        return;
    }

    public static RouteGroupBuilder LivekitApi(this RouteGroupBuilder group)
    {
        group.MapGet("/sessions", (SessionRegistry sessionReg) => sessionReg.GetRoomIds());
        group.MapGet("/clean-sessions", (SessionRegistry sessionReg) =>
        {
            var roomNames = sessionReg.GetRoomIds();
            foreach (var name in roomNames)
            {
                sessionReg.CancelSession(name);
            }
        });
        group.MapPost("/webhook", WebhookCallback);

        return group;
    }

    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.WebHost.UseUrls("http://localhost:5000");

        // TODO: MIgrate to options using DI
        // Single config for whole app for now
        // Check options injection later
        builder
            .Services.AddOptions<LiveKitOptions>()
            .BindConfiguration("")
            .ValidateDataAnnotations()
            .ValidateOnStart();

        builder
            .Services.AddOptions<AudioOptions>()
            .BindConfiguration("")
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // builder.Services.AddOptions<AppOptions>().BindConfiguration(""); // No prefix for compatibility

        // builder.Services.AddSingleton<Watcher>();
        // builder.Services.AddHostedService<WatcherService>();
        builder.Services.AddSingleton<SessionRegistry>();
        builder.Services.AddSingleton<SessionFactory>();
        builder.Services.AddSingleton<Factory<Executor>>();
        builder.Services.AddSingleton<LiveKitSdkUtils>();

        builder.Services.AddTransient<DeepGramConnector>();

        var app = builder.Build();

        if (app.Environment.IsDevelopment())
        {
            Console.WriteLine(">>> Running on Development <<<");
        }
        else
        {
            Console.WriteLine(">>> Running on Production <<<");
        }

        app.MapGroup("/v1/livekit").LivekitApi();

        app.Run();
    }
}
