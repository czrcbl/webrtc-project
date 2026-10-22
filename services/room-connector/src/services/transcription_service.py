import asyncio

from src.services.bridge_service import BridgeService


class BaseTranscriptionService:
    """
    One WebSocket per track: audio goes up, transcripts come back and are
    published through the bridge. Providers fill in how to connect, how to
    signal the end of the audio and how to read their results.
    """

    # Sent once the track ends, so the provider flushes the uncommitted tail.
    END_OF_AUDIO: str

    def __init__(self, bridge_service: BridgeService):
        self.bridge_service: BridgeService = bridge_service

    def start_ws(self):
        """Return an (async context manager) WebSocket connection."""
        raise NotImplementedError

    async def receive_results(self, ws) -> None:
        """Publish the provider's results until it closes the socket."""
        raise NotImplementedError

    async def send_audio(self, ws) -> None:
        try:
            async for ev in self.bridge_service.get_audio_stream():
                await ws.send(bytes(ev.frame.data))
            # Track ended on its own: have the server flush the uncommitted tail.
            await ws.send(self.END_OF_AUDIO)
        finally:
            await self.bridge_service.close_audio_stream()

    async def run(self):
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
