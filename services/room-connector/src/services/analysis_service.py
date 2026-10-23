import asyncio

from anthropic import AsyncAnthropic
from google import genai
from src.logger import logger

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


class GeminiClient:
    model_name = "gemini-3.8-flash"

    def __init__(self):
        self.client = genai.Client(
            http_options=genai.types.HttpOptions(
                timeout=30_000,  # ms, per attempt
                retry_options=genai.types.HttpRetryOptions(attempts=3),  # 408/429/5xx, backoff
            )
        ).aio

    async def get_completion(self, prompt: str, system=None):
        response = await self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                # system_instruction="You are an assistant in charge of summarizing ongoing conversations."
                temperature=0.7,
            ),
        )
        return response.text or ""


class AnthropicClient:
    model_name = "claude-haiku-5-5"

    def __init__(self):
        self.client = AsyncAnthropic()

    async def get_completion(self, prompt, system=None):
        message = await self.client.messages.create(
            model=self.model_name,
            max_tokens=1024,
            # Haiku 5.5 thinks by default; a short summary doesn't need it, and
            # thinking would eat into max_tokens.
            thinking={"type": "disabled"},
            messages=[{"role": "user", "content": prompt}],
        )
        # content may hold non-text blocks (e.g. thinking): keep only the text.
        text = "".join(b.text for b in message.content if b.type == "text")
        if not text:
            logger.warning("anthropic: no text in response, stop_reason=%s", message.stop_reason)
        return text


class AnalysisService:
    def __init__(self, room, transcript_q=None):
        self.room = room
        self.transcript_q = transcript_q or asyncio.Queue()
        self.current_summary = ""
        self.transcripts: list = []
        # Finals not summarized yet; kept across windows so none is lost.
        self.pending: list = []
        self.current_summary = ""
        self.client = AnthropicClient()

    @property
    def queue(self):
        return self.transcript_q

    async def summarize(self):
        while True:
            try:
                await self._pool_transcripts()
                if not self.pending:
                    await publish_chat(self.room, "No new activity for user")
                    continue
                transcripts, self.pending = self.pending, []
                logger.info(
                    f"summarize: extracted N {len(self.transcripts)}, {self.transcripts[:3]}"
                )
                summary = await self._build_summary(transcripts)
                await publish_chat(self.room, summary)
            except asyncio.QueueShutDown:
                logger.info("summarize: queue closed, returning")
                return
            # TODO: Properly handle each error
            # Not bare: cancellation must get through. Keep going, so one failed
            # call doesn't end the summaries for good.
            except Exception:
                logger.exception("summarize: error building summary")
                # Retry these with the next batch.
                self.pending[:0] = transcripts

    async def _pool_transcripts(self):
        """Collect finals into self.pending until there are 6 or the 10 s window ends."""
        try:
            async with asyncio.timeout(10):
                while len(self.pending) <= 5:
                    t = await self.transcript_q.get()
                    logger.info(f"summarize: transcription pooled {t}")
                    # Committed text only: partials repeat the utterance so far, and an
                    # empty final only closes a caption.
                    if t.final and t.text:
                        self.pending.append(t)
        except TimeoutError:
            pass

    async def _build_summary(self, transcripts):
        prompt = """
            Update the summary of an ongoing conversation.
            You will use the previous summary computed and the last several messages.

            Rules:
                - Use the same language as in the conversation.
                - The summary should have three sentences at most.
"""

        if self.current_summary:
            prompt += f"Current Summary {self.current_summary}\n"

        for t in transcripts:
            prompt += f"{t.participant_identity}: {t.text}\n"

        self.current_summary = await self.client.get_completion(prompt)

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
