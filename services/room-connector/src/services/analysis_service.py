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


class SummaryService:
    db = Database
    model_name = "gemini-3.8-flash"
    aclient = genai.Client().aio

    def __init__(self, room):
        self.room = room
        self.current_summary = ""
        self.pointer = 0
        # Shared by every track's transcript broadcaster.
        self.inbox: asyncio.Queue[TranscriptionMessage] = asyncio.Queue(maxsize=INBOX_SIZE)
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

    async def consume_transcripts(self) -> None:
        while True:
            msg = await self.inbox.get()
            if msg.text:
                await self.db.store_text(msg.text, msg.participant_identity)

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.consume_transcripts())
            tg.create_task(self.summarize_loop())

    async def summarize_loop(self):
        while True:
            await asyncio.sleep(10)
            logger.info("Checking summary %s", self.current_summary)
            summary = await self.check_summary()
            if summary:
                await self.publish_chat(summary)
