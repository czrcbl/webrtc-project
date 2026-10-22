from urllib.parse import urlencode

import websockets
from src.config import settings
from src.logger import logger
from src.messages import EOF, StreamParams, TranscriptMessage
from src.services.transcription_service import BaseTranscriptionService


class WhisperService(BaseTranscriptionService):
    """Client for services/stt_server; the protocol is described in src/messages.py."""

    END_OF_AUDIO = EOF

    @property
    def url(self) -> str:
        params = StreamParams(
            language=settings.stt_language,
            participant=self.bridge_service.participant.identity,
            track_id=self.bridge_service.track.sid,
        )
        return f"{settings.stt_ws_url}?{urlencode(params.model_dump(exclude_none=True))}"

    def start_ws(self):
        return websockets.connect(self.url, max_size=None)

    async def receive_results(self, ws) -> None:
        # Returns when the server closes after EOF. A close for bad parameters
        # (1008) or a server failure (1011) raises, with the reason attached.
        async for raw in ws:
            if isinstance(raw, bytes):
                continue
            msg = TranscriptMessage.model_validate_json(raw)

            logger.info(
                "transcript track=%s final=%s: %s",
                self.bridge_service.track.sid,
                msg.final,
                msg.text,
            )

            # A final is one committed segment and closes the caption line; the
            # partial that follows is the uncommitted tail and opens the next.
            await self.bridge_service.publish_transcript(msg.text, final=msg.final)
