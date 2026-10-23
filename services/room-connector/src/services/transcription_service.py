import asyncio
from typing import Literal

from src.broadcaster import Broadcaster
from src.messages import AudioStreamMessage, TranscriptionMessage


class BaseTranscriptionService:
    """
    One WebSocket per track: audio frames are read from `audio_in` and sent up,
    transcripts come back and are put on `transcripts_out`. Providers fill in
    how to connect, how to signal the end of the audio and how to read their
    results.
    """

    PROVIDER: Literal["deepgram", "whisper"]
    # Sent once the track ends, so the provider flushes the uncommitted tail.
    END_OF_AUDIO: str

    def __init__(
        self,
        track_sid: str,
        participant_identity: str,
        audio_in: asyncio.Queue[AudioStreamMessage],
        transcripts_out: asyncio.Queue | Broadcaster,
    ):
        self.track_sid = track_sid
        self.participant_identity = participant_identity
        self.audio_in = audio_in
        self.transcripts_out = transcripts_out

    def start_ws(self):
        """Return an (async context manager) WebSocket connection."""
        raise NotImplementedError

    async def receive_results(self, ws) -> None:
        """Emit the provider's results until it closes the socket."""
        raise NotImplementedError

    async def emit(self, text: str, final: bool) -> None:
        await self.transcripts_out.put(
            TranscriptionMessage(
                track_sid=self.track_sid,
                participant_identity=self.participant_identity,
                text=text,
                final=final,
                provider=self.PROVIDER,
            )
        )

    async def send_audio(self, ws) -> None:
        while True:
            try:
                msg = await self.audio_in.get()
            except asyncio.QueueShutDown:
                break
            await ws.send(msg.pcm)
        # Track ended on its own: have the server flush the uncommitted tail.
        await ws.send(self.END_OF_AUDIO)

    async def run(self):
        try:
            async with self.start_ws() as ws:
                try:
                    tasks = [
                        asyncio.create_task(self.send_audio(ws)),
                        asyncio.create_task(self.receive_results(ws)),
                    ]
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                    for t in done:
                        t.result()
                finally:
                    for t in tasks:
                        t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            # No more transcripts: the Bridge drains what is queued and finishes.
            self.transcripts_out.shutdown()
