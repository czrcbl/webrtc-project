import secrets

from livekit import rtc
from src.config import settings
from src.logger import logger


class BridgeService:
    def __init__(
        self,
        room,
        track,
        participant,
    ):
        self.room = room
        self.track = track
        self.participant = participant

        self._stream: rtc.AudioStream | None = None
        # TODO: nonce might be unnecessary,
        # watch if the turn repeating was not a bug on a previous implementation
        self._nonce: str = secrets.token_hex(3)
        self._turn: int = 0

    def get_audio_stream(self):
        if self._stream is None:
            self._stream = rtc.AudioStream(
                self.track,
                sample_rate=settings.sample_rate,
                num_channels=settings.CHANNELS,
                frame_size_ms=settings.send_chunk_ms,
            )
        return self._stream

    async def close_audio_stream(self):
        if self._stream is not None:
            await self._stream.aclose()
            self._stream = None

    async def publish_transcript(
        self,
        transcript_text: str,
        final: bool,
    ) -> None:
        logger.info(
            "Publishing to track %s, identity %s", self.track.sid, self.participant.identity
        )
        logger.info("Text received %s", transcript_text)

        if final:
            self._turn += 1
            # TODO: Check stream text necessity
        writer = await self.room.local_participant.stream_text(
            topic="lk.transcription",
            sender_identity=self.participant.identity,
            attributes={
                "lk.transcribed_track_id": self.track.sid,
                # Prefixed with the track: Captions.tsx groups across all speakers.
                "lk.segment_id": f"{self.track.sid}-{self._nonce}-{self._turn}",
                "lk.transcription_final": "true" if final else "false",
            },
        )

        try:
            await writer.write(transcript_text)
        finally:
            await writer.aclose()
