using System.Threading.Channels;

using LiveKit.Rtc;

public class Executor(Room room, Participant participant, Track track)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;


    public async Task RunAsync()
    {

        // Channel<byte[]> audioStream = Channel.CreateUnbounded<byte[]>();
        // For a single consumer, keep the original stream
        var stream = new AudioStream(_track, Configs.sampleRate, Configs.numChannels);

        var deepgramService = new DeepgramService(stream);

        await deepgramService.StartAsync();
        await deepgramService.RunAsync();

    }
}