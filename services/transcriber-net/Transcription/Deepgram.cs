using Deepgram;
using Deepgram.Models.Listen.v2.WebSocket;

public class DeepGramTranscriber(ILogger<DeepGramTranscriber> logger) : BaseTranscriber
{
    public event EventHandler<TranscriptionMessage>? TranscriptReceived;
    LiveSchema _liveSchema = new()
    {
        Model = "nova-3",
        Encoding = "linear16",
        SampleRate = (int)Configs.sampleRate,
        Channels = (int)Configs.numChannels,
        Language = "pt-BR",
    };

    private ListenWebSocketClient _liveClient = (ListenWebSocketClient)
        ClientFactory.CreateListenWebSocketClient();

    public async Task ConnectClientAsync()
    {
        // TODO: how to handle disconnect
        await SubscribeDeepGramAsync();
        await _liveClient.Connect(_liveSchema);
        await _liveClient.SendConfigure(
            new ConfigureSchema
            {
                Keyterms = new List<string> { "Deepgram" },
                Features = new Dictionary<string, bool> { ["numerals"] = true },
            }
        );
    }

    private void OnResult(object? sender, ResultResponse e)
    {
        logger.LogInformation("Result Received");
        var alternatives = e.Channel?.Alternatives;
        if (alternatives is not null)
        {
            var text = alternatives.First().Transcript;
            if (text is not null)
            {
                var message = new TranscriptionMessage(text, e.IsFinal ?? false);
                logger.LogInformation($"Received Text {message.Text}");
                TranscriptReceived?.Invoke(this, message);
            }
        }
    }

    public void Send(byte[] audioData)
    {
        _liveClient.Send(audioData);
    }

    public async Task StopAsync()
    {
        await _liveClient.Stop();
    }

    private async Task SubscribeDeepGramAsync()
    {
        await _liveClient.Subscribe(new EventHandler<ResultResponse>(OnResult));
    }
}
