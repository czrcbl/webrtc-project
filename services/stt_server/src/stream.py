"""Per-connection state: buffers the PCM and turns each decode into partials and finals."""

from dataclasses import dataclass, field

import numpy as np

from .config import settings
from .engine import WhisperEngine
from .messages import SAMPLE_RATE, TranscriptMessage

# With only silence so far, past this much buffered audio everything but a short
# tail is dropped: the tail keeps a word that is just starting from being cut.
SILENCE_TRIM_AFTER_S = 5.0
SILENCE_KEEP_S = 0.5


@dataclass
class Stream:
    engine: WhisperEngine
    language: str | None
    task: str
    buffer: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.float32))
    offset_s: float = 0.0  # absolute time of buffer[0] within the stream
    pending_new: int = 0  # samples received since the last decode
    carry: bytes = b""  # odd trailing byte from a split sample
    committed: list[str] = field(default_factory=list)

    def add_pcm16(self, data: bytes) -> None:
        data = self.carry + data
        if len(data) % 2:
            data, self.carry = data[:-1], data[-1:]
        else:
            self.carry = b""
        samples = np.frombuffer(data, dtype="<i2").astype(np.float32) / 32768.0
        self.buffer = np.concatenate([self.buffer, samples])
        self.pending_new += len(samples)

    @property
    def buffer_s(self) -> float:
        return len(self.buffer) / SAMPLE_RATE

    @property
    def decode_due(self) -> bool:
        return self.pending_new >= settings.decode_every_s * SAMPLE_RATE

    def prompt(self) -> str:
        return " ".join(self.committed)[-settings.prompt_chars :]

    async def decode(self, final: bool) -> list[TranscriptMessage]:
        """
        Re-transcribe the uncommitted audio. Returns the segments committed by
        this pass as finals, then the remaining tail as a partial. With final=True
        everything is committed, for the end of the stream.
        """
        self.pending_new = 0
        if len(self.buffer) == 0:
            return []

        segs = await self.engine.transcribe(
            self.buffer.copy(), self.language, self.task, self.prompt()
        )
        length = self.buffer_s

        if not segs:
            # Only silence so far: drop it to keep decodes cheap.
            if not final and length > SILENCE_TRIM_AFTER_S:
                keep = int(SILENCE_KEEP_S * SAMPLE_RATE)
                self.offset_s += (len(self.buffer) - keep) / SAMPLE_RATE
                self.buffer = self.buffer[-keep:]
            return []

        if final:
            n_commit = len(segs)
        else:
            # Commit segments that are followed by another one and end well before
            # the buffer edge; Whisper rarely revises those.
            n_commit = 0
            for i, s in enumerate(segs[:-1]):
                if s.end <= length - settings.stable_margin_s:
                    n_commit = i + 1
            # Long monologue with no break: commit everything but the last segment,
            # or the whole thing if it's a single segment.
            if n_commit == 0 and length >= settings.max_buffer_s:
                n_commit = max(len(segs) - 1, 1)

        messages = []
        for s in segs[:n_commit]:
            if s.text:
                self.committed.append(s.text)
                messages.append(TranscriptMessage(text=s.text, final=True))

        if n_commit:
            cut = min(int(segs[n_commit - 1].end * SAMPLE_RATE), len(self.buffer))
            self.buffer = self.buffer[cut:]
            self.offset_s += cut / SAMPLE_RATE

        partial = " ".join(s.text for s in segs[n_commit:] if s.text)
        if partial and not final:
            messages.append(TranscriptMessage(text=partial, final=False))
        return messages
