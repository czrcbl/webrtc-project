public record TranscriptionMessage(string Text, bool IsFinal);

public interface ITranscriber { }

public abstract class BaseTranscriber : ITranscriber { }
