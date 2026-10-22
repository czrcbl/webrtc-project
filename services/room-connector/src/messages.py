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
"""

from typing import Literal

from pydantic import BaseModel, field_validator

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
