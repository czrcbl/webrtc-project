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
        # Deepgram's is_final locks a chunk of audio, not an utterance: keep the
        # locked chunks until speech_final / UtteranceEnd and emit them as one final.
        self._chunks: list[str] = []
        self._partial_shown = False  # a caption is open and must be closed by a final

    @property
    def headers(self):
        return {"Authorization": f"Token {settings.deepgram_api_key.get_secret_value()}"}

    def start_ws(self):
        return websockets.connect(self.URL, additional_headers=self.headers)

    async def close_ws(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def _show_partial(self, interim: str = "") -> None:
        text = " ".join([*self._chunks, interim]).strip()
        if text:
            await self.emit(text, final=False)
            self._partial_shown = True

    async def _end_utterance(self) -> None:
        # The final replaces the partials under the same segment; "" only to close
        # a caption whose words Deepgram dropped on finalising.
        text = " ".join(self._chunks)
        if text or self._partial_shown:
            await self.emit(text, final=True)
        self._chunks.clear()
        self._partial_shown = False

    async def receive_results(self, ws) -> None:
        """Downlink: emit partials and one final per utterance. Returns on done."""
        async for msg in ws:
            if isinstance(msg, bytes):
                continue
            data = json.loads(msg)
            kind = data.get("type")

            if kind == "Results":
                alternatives = (data.get("channel") or {}).get("alternatives") or []
                transcript = alternatives[0].get("transcript", "") if alternatives else ""
                is_final = bool(data.get("is_final"))
                speech_final = bool(data.get("speech_final"))

                logger.info(
                    "transcript track=%s is_final=%s speech_final=%s: %s",
                    self.track_sid,
                    is_final,
                    speech_final,
                    transcript,
                )

                # Partials are only displayed, never stored: the next ones (and
                # the final that locks them) cover the same audio.
                if not is_final:
                    await self._show_partial(transcript)
                    continue
                if transcript:  # empty finals are silence
                    self._chunks.append(transcript)
                if speech_final:
                    await self._end_utterance()
                else:
                    await self._show_partial()  # locked so far, utterance continues

            elif kind == "UtteranceEnd":
                await self._end_utterance()
            else:
                logger.info("Non Result message %s", kind)

        # CloseStream flushed the last finals and Deepgram closed: commit the tail.
        await self._end_utterance()
