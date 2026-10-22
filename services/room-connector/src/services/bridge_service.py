import asyncio
import secrets

from google import genai
from livekit import rtc
from src.config import settings
from src.logger import logger


class Database:
    # TODO: review this, if dispatching to more than one Room,
    # conversations will get mixed on the same process
    memory = []
    lock = asyncio.Lock()

    @classmethod
    async def store_text(cls, message: str, user_identity):
        """Store only the transcript for now"""
        async with cls.lock:
            cls.memory.append((message, user_identity))

    @classmethod
    async def get_memory(cls):
        async with cls.lock:
            for e in cls.memory:
                yield e


class SummaryService:
    db = Database
    model_name = "gemini-3.8-flash"
    aclient = genai.Client().aio

    def __init__(self, room):
        self.room = room
        self.current_summary = ""
        # In memory for now

    async def summarize(self):
        prompt = """
            Update the summary of an ongoing conversation.
            You will use the previous summary computed and the last several messages.

            Rules:
                - Use the same language as in the conversation.
                - The summary should have three sentences at most.
"""

        if self.current_summary:
            prompt += f"Current Summary {self.current_summary}\n"

        async for message, user in self.db.get_memory():
            prompt += f"{user}: {message}\n"

        response = await self.aclient.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction="You are an assistant in charge of summarizing ongoing conversations.",
                temperature=0.7,
            ),
        )
        self.db.memory = []
        self.current_summary = response.text or ""

        return self.current_summary

    async def check_summary(self):
        if len(self.db.memory) < 10:
            return None
        return await self.summarize()

    async def publish_chat(
        self,
        message,
        destination_identities: list[str] | None = None,
    ) -> None:
        """Send a chat message to the room as the agent itself."""
        logger.info(
            "Publishing chat as %s: %s",
            self.room.local_participant.identity,
        )
        await self.room.local_participant.send_text(
            message,
            topic="lk.chat",
            destination_identities=destination_identities,  # None = everyone
        )

    async def run(self):
        while True:
            await asyncio.sleep(10)
            logger.info("Checking summary %s", self.current_summary)
            summary = await self.check_summary()
            if summary:
                await self.publish_chat(summary)


class BridgeService:
    db = Database

    def __init__(self, room, track, participant):
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

        if transcript_text and self.db:
            await self.db.store_text(
                transcript_text,
                self.participant.identity,
            )

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
