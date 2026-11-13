using Livekit.Server.Sdk.Dotnet;

using Microsoft.Extensions.Options;
/* Application lifecycle, check latter
Microsoft.Extensions.Hosting
Also, ASP.net equivalent.
*/

public static class Program
{

    private static void RegisterWebhook(WebApplication app)
    {

        var receiver = new WebhookReceiver(Configs.liveKitApiKey, Configs.liveKitApiSecret);

        // ASP.NET Core minimal API. Pass the raw body: LiveKit signs its SHA-256.
        app.MapPost("/livekit/webhook", async (HttpRequest request, Watcher w) =>
        {
            using var body = new MemoryStream();
            await request.Body.CopyToAsync(body);

            WebhookEvent webhookEvent = receiver.Receive(
                body.ToArray(),
                request.Headers.Authorization.ToString()
            );

            Console.WriteLine($"{webhookEvent.Event} in room {webhookEvent.Room?.Name}");
            if (webhookEvent.Event == "room_started")
            {
                if (webhookEvent.Room is not null)
                    await w.StartSessionFromHookAsync(webhookEvent.Room.Name);
                else
                    Console.Error.WriteLine($"Room name is null on `room_started` event");
            }

        });
    }

    public static async Task Main(string[] args)
    {

        var builder = WebApplication.CreateBuilder(args);

        builder.WebHost.UseUrls("http://localhost:5000");

        // Single config for whole app for now
        // Check options injection later
        // builder.Services
        //     .AddOptions<Configs>()
        //     .Bind(builder.Configuration)
        //     .ValidateDataAnnotations()
        //     .ValidateOnStart();

        builder.Services.AddOptions<AppOptions>()
            .BindConfiguration("");  // No prefix for compatibility

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

        app.MapGet("/v1/sessions",
            (Watcher w) => w.GetRoomIds());


        RegisterWebhook(app);


        app.Run();
    }

}

