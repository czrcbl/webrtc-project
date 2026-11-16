using System.Threading.Channels;
using LiveKit.Rtc;

// TODO: Review, use arg or class or constructor
public class BridgeService(
    Room room,
    Participant participant,
    Track track,
    Channel<TranscriptionMessage> transcriptionCh,
    CancellationToken ct
)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;

    private Channel<TranscriptionMessage> _transcriptCh = transcriptionCh;

    private CancellationToken _ct = ct;

    public async Task RunAsync()
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var msg = await _transcriptCh.Reader.ReadAsync(_ct);
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
    }
}

// TODO: evaluate Factory Pattern
public class Executor(Room room, Participant participant, Track track, CancellationToken ct)
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
        var stream = new AudioStream(_track, Configs.sampleRate, Configs.numChannels);

        var transcriptChannel = Channel.CreateUnbounded<TranscriptionMessage>();

        var deepgramService = new DeepgramService(stream, transcriptChannel, _ct);
        var bridgeService = new BridgeService(_room, _participant, _track, transcriptChannel, _ct);

        try
        {
            var deept = deepgramService.RunAsync();
            var bridget = bridgeService.RunAsync();
            await Task.WhenAll(deept, bridget).WaitAsync(_ct);
        }
        catch { }

        transcriptChannel.Writer.Complete();
        stream.Dispose();
    }
}
