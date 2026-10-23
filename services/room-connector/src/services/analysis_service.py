import asyncio

from google import genai
from src.logger import logger
from src.messages import TranscriptionMessage

# Finals waiting to be stored; past this the oldest are dropped (see Broadcaster).
INBOX_SIZE = 500


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


async def publish_chat(
    room,
    message: str,
    destination_identities: list[str] | None = None,
) -> None:
    """Send a chat message to the room as the agent itself."""
    logger.info(
        "Publishing chat as %s: %s",
        room.local_participant.identity,
        message,
    )
    await room.local_participant.send_text(
        message,
        topic="lk.chat",
        destination_identities=destination_identities,  # None = everyone
    )


class AnalysisService:
    model_name = "gemini-3.8-flash"
    # TODO: review client lifecycle
    aclient = genai.Client().aio

    def __init__(self, room, transcript_q):
        self.room = room
        self.transcript_q = transcript_q
        self.current_summary = ""
        self.transcripts: list = []
        self.current_summary = ""

    async def summarize(self):
        while True:
            try:
                transcripts = await self._pool_transcripts()
                self.transcripts = transcripts
                logger.info(f"summarize: extracted N {len(transcripts)}, {transcripts[:3]}")
                summary = await self._build_summary()
                await publish_chat(self.room, summary)
            except asyncio.QueueShutDown:
                logger.exception("summarize: queue close, returning")
                return
            # TODO: Properly handle each error
            except:
                logger.exception("summarize: error building summary")
                return

    async def _pool_transcripts(self):
        transcripts = []
        while True:
            t = await self.transcript_q.get()
            transcripts.append(t)
            if len(transcripts) >= 10:
                return transcripts

    async def _build_summary(self):
        prompt = """
            Update the summary of an ongoing conversation.
            You will use the previous summary computed and the last several messages.

            Rules:
                - Use the same language as in the conversation.
                - The summary should have three sentences at most.
"""

        if self.current_summary:
            prompt += f"Current Summary {self.current_summary}\n"

        for t in self.transcripts:
            prompt += f"{t.participant_identity}: {t.text}\n"

        response = await self.aclient.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction="You are an assistant in charge of summarizing ongoing conversations.",
                temperature=0.7,
            ),
        )
        self.current_summary = response.text or ""

        return self.current_summary

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

    # async def consume_transcripts(self) -> None:
    #     while True:
    #         msg = await self.inbox.get()
    #         if msg.text:
    #             await self.db.store_text(msg.text, msg.participant_identity)

    # async def run(self):
    #     async with asyncio.TaskGroup() as tg:
    #         tg.create_task(self.consume_transcripts())
    #         tg.create_task(self.summarize_loop())

    # async def summarize_loop(self):
    #     while True:
    #         await asyncio.sleep(10)
    #         logger.info("Checking summary %s", self.current_summary)
    #         summary = await self.check_summary()
    #         if summary:
    #             await self.publish_chat(summary)
