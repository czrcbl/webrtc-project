import json
from urllib.parse import urlencode

import websockets
from src.config import settings
from src.logger import logger
from src.services.transcription_service import BaseTranscriptionService

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


class DeepgramService(BaseTranscriptionService):
    PROVIDER = "deepgram"
    URL = DG_URL
    END_OF_AUDIO = json.dumps({"type": "CloseStream"})

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

    async def receive_results(self, ws) -> None:
        """Downlink: turn service messages into LiveKit text streams. Returns on done."""
        # Avoid reusing segment

        async for msg in ws:
            if isinstance(msg, bytes):
                continue
            data = json.loads(msg)
            # logger.info(f"Received {data}")

            if data.get("type") == "Results":
                alternatives = (data.get("channel") or {}).get("alternatives") or []
                transcript = alternatives[0].get("transcript") if alternatives else ""
                is_final = bool(data.get("is_final"))

                logger.info(
                    "transcript track=%s is_final=%s speech_final=%s: %s",
                    self.track_sid,
                    is_final,
                    data.get("speech_final"),
                    transcript,
                )

                if is_final:
                    await self.emit(transcript, final=True)
                elif transcript:
                    await self.emit(transcript, final=False)
            else:
                logger.info("Non Result message %s", data.get("type"))
