using LiveKit.Rtc;

public class Executor(Room room, Participant participant, Track track)
{
    private Room _room = room;
    private Participant _participant = participant;

    private Track _track = track;


    public async Task RunAsync()
    {
        var stream = new AudioStream(_track, Configs.sampleRate, Configs.numChannels);

        await foreach (var chunk in stream)
        {
            Console.Write($"{chunk.Frame.DataArray.Length}");
        }
    }
}