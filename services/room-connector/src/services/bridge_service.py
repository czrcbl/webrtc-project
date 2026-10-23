import asyncio
import secrets

from livekit import rtc
from src.config import settings
from src.logger import logger
from src.messages import AudioStreamMessage


class BridgeService:
    def __init__(self, room, track, participant, transcript_q):
        self.room = room
        self.track = track
        self.participant = participant
        self.transcript_q = transcript_q

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

    async def produce_audio(self, out: asyncio.Queue[AudioStreamMessage]) -> None:
        """Put the track's PCM frames on `out`; shuts it down when the track ends."""
        try:
            seq = 0
            async for ev in self.get_audio_stream():
                await out.put(
                    AudioStreamMessage(
                        track_sid=self.track.sid,
                        participant_identity=self.participant.identity,
                        seq=seq,
                        pcm=bytes(ev.frame.data),
                        sample_rate=ev.frame.sample_rate,
                        channels=ev.frame.num_channels,
                    )
                )
                seq += 1
        finally:
            # Track ended (or we were cancelled): the STT side flushes and finishes.
            out.shutdown()
            await self.close_audio_stream()

    async def consume_transcripts(self) -> None:
        """Publish transcripts from `inp` until the STT side shuts it down."""
        while True:
            try:
                msg = await self.transcript_q.get()
            except asyncio.QueueShutDown:
                return
            await self.publish_transcript(msg.text, final=msg.final)

    async def publish_transcript(
        self,
        transcript_text: str,
        final: bool,
    ) -> None:
        # The final goes out under the same id as the partials it commits; only
        # then does the turn advance, so the next partial opens a new segment.
        segment_id = f"{self.track.sid}-{self._nonce}-{self._turn}"
        if final:
            self._turn += 1

        logger.info(
            "Publishing segment %s (final=%s) for identity %s: %s",
            segment_id,
            final,
            self.participant.identity,
            transcript_text,
        )

        # TODO: Check stream text necessity
        writer = await self.room.local_participant.stream_text(
            topic="lk.transcription",
            sender_identity=self.participant.identity,
            attributes={
                "lk.transcribed_track_id": self.track.sid,
                # Prefixed with the track: Captions.tsx groups across all speakers.
                "lk.segment_id": segment_id,
                "lk.transcription_final": "true" if final else "false",
            },
        )

        try:
            await writer.write(transcript_text)
        finally:
            await writer.aclose()

    async def publish_chat(
        self,
        message: str,
        destination_identities: list[str] | None = None,
    ) -> None:
        """Send a chat message to the room as the agent itself."""
        logger.info(
            "Publishing chat as %s: %s",
            self.room.local_participant.identity,
            message,
        )
        await self.room.local_participant.send_text(
            message,
            topic="lk.chat",
            destination_identities=destination_identities,  # None = everyone
        )
