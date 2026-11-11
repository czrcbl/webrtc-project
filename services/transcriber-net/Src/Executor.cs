using System.Threading.Channels;

using LiveKit.Rtc;


// TODO: Review, use arg or class or constructor
public class BridgeService(Room room, Participant participant, Channel<string> transcriptionCh)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Channel<string> _transcriptCh = transcriptionCh;

    public async Task RunAsync()
    {
        while (true)
        {
            var msg = await _transcriptCh.Reader.ReadAsync();
            Console.WriteLine($"Bridge: message received {msg}");
            await _room.LocalParticipant.SendTextAsync(
                msg, "lk.transcription");
            // new Dictionary<string, bool> { ["numerals"] = true },
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

        Channel<string> transcriptChannel = Channel.CreateUnbounded<string>();

        var deepgramService = new DeepgramService(stream, transcriptChannel);
        var bridgeService = new BridgeService(_room, _participant, transcriptChannel);

        await deepgramService.StartAsync();


        var deept = deepgramService.RunAsync();
        var bridget = bridgeService.RunAsync();

        Task.WaitAll([deept, bridget]);

    }
}