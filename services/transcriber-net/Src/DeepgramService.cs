using LiveKit.Rtc;

using Deepgram;
using Deepgram.Clients.Agent.v2.WebSocket;
using Deepgram.Models.Listen.v2.WebSocket;
// using Deepgram.Models.Speak.v2.WebSocket;
// using Deepgram.Models.Agent.v2.WebSocket;

public class DeepgramService
{

    private AudioStream _stream;
    private ListenWebSocketClient _liveClient = (ListenWebSocketClient)ClientFactory.CreateListenWebSocketClient();

    public DeepgramService(AudioStream stream)
    {
        _stream = stream;
    }

    // private void ConsoleCallback(EventSourcesender, e)
    // {
    //     if (!string.IsNullOrEmpty(e.Channel.Alternatives[0].Transcript))
    //     {
    //         Console.WriteLine($"Transcript: {e.Channel.Alternatives[0].Transcript}");
    //     }
    // }


    // Based on Example code on the SDk
    public async Task StartAsync()
    {
        await _liveClient.Subscribe(
    new EventHandler<ResultResponse>((sender, e) =>
    {
        // Console.Error.WriteLine($"{e.Variant} {e.Code}: {e.Description}");
        var alternatives = e.Channel?.Alternatives;
        if (alternatives is not null)
        {
            var msg = alternatives.First().Transcript;
            Console.Error.Write($"{msg} ");
        }

    }));

        var liveSchema = new LiveSchema()
        {
            Model = "nova-3",
            Encoding = "linear16",
            SampleRate = (int)Configs.sampleRate,
            Channels = (int)Configs.numChannels,
            Language = "pt-BR"
        };
        await _liveClient.Connect(liveSchema);
        await _liveClient.SendConfigure(new ConfigureSchema
        {
            Keyterms = new List<string> { "Deepgram" },
            Features = new Dictionary<string, bool> { ["numerals"] = true },
        });

    }

    public async Task RunAsync()
    {
        await StartAsync();
        await foreach (var chunk in _stream)
        {
            // Console.Write($"{chunk.Frame.DataArray.Length}");
            Send(chunk.Frame.DataBytes);

        }
        await StopAsync();
    }

    public void Send(byte[] audioData)
    {
        _liveClient.Send(audioData);
    }

    public async Task StopAsync()
    {
        await _liveClient.Stop();
    }

}

// await liveClient.Stop();