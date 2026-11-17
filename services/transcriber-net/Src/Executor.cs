using System.Threading.Channels;
using LiveKit.Rtc;

// TODO: Review, use arg or class or constructor
public class BridgeService(Room room, Participant participant, Track track, CancellationToken ct)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;

    private Channel<TranscriptionMessage> _transcriptCh =
        Channel.CreateUnbounded<TranscriptionMessage>();

    private CancellationToken _ct = ct;

    public async Task SendTranscript(TranscriptionMessage msg)
    {
        try
        {
            var writer = await _room.LocalParticipant.StreamTextAsync(
                "lk.transcription",
                new Dictionary<string, string>
                {
                    ["lk.transcribed_track_id"] = _track.Sid,
                    ["lk.transcript_final"] = msg.IsFinal ? "true" : "false",
                },
                null,
                null,
                _participant.Identity
            );

            Console.WriteLine($"Bridge: message received {msg.Text}, final? {msg.IsFinal}");

            await writer.WriteAsync(msg.Text);
            await writer.CloseAsync();
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("Bridge: Error writing msg");
        }
    }

    public void HandleTranscriptEvent(object? sender, TranscriptionMessage msg)
    {
        Task.Run(
            () =>
            {
                _transcriptCh.Writer.TryWrite(msg);
            },
            _ct
        );
    }

    public async Task RunAsync()
    {
        await foreach (var msg in _transcriptCh.Reader.ReadAllAsync(_ct))
        {
            await SendTranscript(msg);
            if (ct.IsCancellationRequested)
                break;
        }
        _transcriptCh.Writer.TryComplete();
    }
}

// TODO: evaluate Factory Pattern
public class Executor(
    Room room,
    Participant participant,
    Track track,
    CancellationToken ct,
    DeepGramConnector transcriber,
    ILogger<Executor> logger
)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;

    private CancellationToken _ct = ct;

    public async Task RunAsync()
    {
        Console.WriteLine("Executor Started");
        // Channel<byte[]> audioStream = Channel.CreateUnbounded<byte[]>();
        // For a single consumer, keep the original stream
        var bridgeService = new BridgeService(_room, _participant, _track, _ct);

        transcriber.TranscriptReceived += bridgeService.HandleTranscriptEvent;

        await transcriber.ConnectClientAsync();
        var bridgeTask = bridgeService.RunAsync();

        var stream = new AudioStream(_track, Configs.sampleRate, Configs.numChannels);
        await foreach (var chunk in stream)
        {
            // Console.Write($"{chunk.Frame.DataArray.Length}");
            transcriber.Send(chunk.Frame.DataBytes);
            if (ct.IsCancellationRequested)
                break;
        }
        await transcriber.StopAsync();
        stream.Dispose();
        bridgeTask.Dispose();

        // try
        // {
        //     // var deept = deepgramService.RunAsync();
        //     var bridget = bridgeService.RunAsync();
        //     // await Task.WhenAll(deept, bridget).WaitAsync(_ct);
        // }
        // // TODO: Build exceptions
        // catch (Exception e) { }
    }
}
