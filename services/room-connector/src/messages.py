"""
Wire protocol between stt_server and its clients (one WebSocket = one audio stream).

    Connect:          ws://HOST:8000/ws/transcribe?language=pt&task=transcribe
                      Every parameter is in the query string (see StreamParams);
                      there is no handshake, audio can be sent right away.
    Client -> server: binary frames of raw PCM, signed 16-bit little-endian,
                      mono, at SAMPLE_RATE. The text frame EOF ends the stream.
    Server -> client: TranscriptMessage, as JSON text frames. After EOF the server
                      flushes the remaining finals and closes (code 1000). Bad
                      parameters close with 1008, server failures with 1011; the
                      close reason says why.

Keep in sync with services/stt_server/src/messages.py until wrtc-common exists.

Internal messages (in-process, room-connector only), passed over asyncio queues
by ExecutorService:
    Bridge -> STT:    AudioStreamMessage, one PCM16 frame of a track.
    STT -> Bridge:    TranscriptionMessage, a partial or final transcript.
    Any -> Bridge:    ChatMessage, a chat line the agent posts on lk.chat.
    The producer ends a stream with Queue.shutdown(); consumers drain what is
    left and then get QueueShutDown.
"""

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SAMPLE_RATE = 16_000
EOF = "eof"


class StreamParams(BaseModel):
    """Query parameters; unknown ones are ignored."""

    language: str | None = None  # ISO code; omitted or empty -> auto-detect
    task: Literal["transcribe", "translate"] = "transcribe"
    # Only used to label the server logs.
    participant: str | None = None
    track_id: str | None = None

    @field_validator("language", mode="before")
    @classmethod
    def _empty_is_auto(cls, v: str | None) -> str | None:
        return v or None


class TranscriptMessage(BaseModel):
    """A partial (final=False) is replaced by the next message; a final is committed."""

    text: str
    final: bool = False


class AudioStreamMessage(BaseModel):
    """One PCM16 frame from the Bridge to an STT service."""

    model_config = ConfigDict(frozen=True)

    track_sid: str
    participant_identity: str
    seq: int  # increases by one per frame within a track
    pcm: bytes  # signed 16-bit little-endian
    sample_rate: int = SAMPLE_RATE
    channels: int = 1
    captured_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TranscriptionMessage(BaseModel):
    """A transcript from an STT service to the Bridge; partials are replaced, finals committed."""

    model_config = ConfigDict(frozen=True)

    track_sid: str
    participant_identity: str
    text: str
    final: bool = False
    provider: Literal["deepgram", "whisper"]
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ChatMessage(BaseModel):
    """A chat line for the agent to post on lk.chat."""

    model_config = ConfigDict(frozen=True)

    text: str
    destination_identities: list[str] | None = None  # None -> everyone in the room
    source: Literal["summary"] = "summary"  # which service produced it
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
