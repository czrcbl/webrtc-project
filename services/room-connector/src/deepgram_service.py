import asyncio
import json
from urllib.parse import urlencode

import websockets
from src.bridge_service import BridgeService
from src.config import settings
from src.logger import logger

# Fixed by what send_audio produces (mono pcm_s16le), so not configurable.


DG_PARAMS = {
    "model": settings.deepgram_model,
    "language": settings.deepgram_language,
    "encoding": "linear16",
    "sample_rate": settings.sample_rate,
    "channels": settings.CHANNELS,
    "interim_results": str(settings.deepgram_interim_results).lower(),
    "smart_format": str(settings.deepgram_smart_format).lower(),
    "endpointing": settings.deepgram_endpointing_ms,
    "utterance_end_ms": settings.deepgram_utterance_end_ms,
}
DG_URL = "wss://api.deepgram.com/v1/listen?" + urlencode(DG_PARAMS)


class BaseTranscriptionService:
    def __init__(self, bridge_service: BridgeService):
        self.bridge_service: BridgeService = bridge_service


class DeepgramService(BaseTranscriptionService):
    URL = DG_URL

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def headers(self):
        return {"Authorization": f"Token {settings.deepgram_api_key.get_secret_value()}"}

    def start_ws(self):
        return websockets.connect(self.URL, additional_headers=self.headers)

    async def close_ws(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def send_audio(self, ws) -> None:
        try:
            async for ev in self.bridge_service.get_audio_stream():
                await ws.send(bytes(ev.frame.data))
            # Track ended on its own: have the server flush the uncommitted tail.
            await ws.send(json.dumps({"type": "CloseStream"}))
        finally:
            await self.bridge_service.close_audio_stream()

    async def receive_results(self, ws) -> None:
        """Downlink: turn service messages into LiveKit text streams. Returns on done."""
        # Avoid reusing segment

        async for msg in ws:
            if isinstance(msg, bytes):
                continue
            data = json.loads(msg)
            logger.info(f"Received {data}")

            if data.get("type") == "Results":
                alternatives = (data.get("channel") or {}).get("alternatives") or []
                transcript = alternatives[0].get("transcript") if alternatives else ""
                is_final = data.get("is_final")

                logger.info("transcript %s", transcript)

                if transcript:
                    await self.bridge_service.publish_transcript(
                        transcript,
                        final=False,
                    )
                elif is_final:
                    await self.bridge_service.publish_transcript(
                        transcript,
                        final=True,
                    )
            else:
                logger.info("Non Result message %s", data.get("type"))

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
