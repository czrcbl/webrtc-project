using System.Threading.Channels;

using LiveKit.Rtc;


// TODO: Review, use arg or class or constructor
public class BridgeService(Room room, Participant participant, Track track, Channel<TranscriptionMessage> transcriptionCh)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;

    private Channel<TranscriptionMessage> _transcriptCh = transcriptionCh;

    public async Task RunAsync()
    {
        while (true)
        {
            try
            {
                var msg = await _transcriptCh.Reader.ReadAsync();
                var writer = await _room.LocalParticipant.StreamTextAsync(
                "lk.transcription",
                    new Dictionary<string, string>
                    { ["lk.transcribed_track_id"] = _track.Sid, ["lk.transcript_final"] = msg.IsFinal ? "true" : "false" },
                    null, null, _participant.Identity);

                Console.WriteLine($"Bridge: message received {msg.Text}, final? {msg.IsFinal}");

                // new Dictionary<string, bool> { ["numerals"] = true },
                await writer.WriteAsync(msg.Text);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Bridge: Error writing msg");

                break;
            }
        }

    }
}
public class Executor(Room room, Participant participant, Track track)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;


    public async Task RunAsync()
    {
        Console.WriteLine("Executor Started");
        // Channel<byte[]> audioStream = Channel.CreateUnbounded<byte[]>();
        // For a single consumer, keep the original stream
        var stream = new AudioStream(_track, Configs.sampleRate, Configs.numChannels);

        var transcriptChannel = Channel.CreateUnbounded<TranscriptionMessage>();

        var deepgramService = new DeepgramService(stream, transcriptChannel);
        var bridgeService = new BridgeService(_room, _participant, _track, transcriptChannel);

        await deepgramService.StartAsync();


        var deept = deepgramService.RunAsync();
        var bridget = bridgeService.RunAsync();

        Task.WaitAll([deept, bridget]);

    }
}