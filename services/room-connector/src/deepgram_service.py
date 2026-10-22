import json
import secrets
from urllib.parse import urlencode

import websockets
from livekit import rtc
from src.config import settings
from src.logger import logger

# Fixed by what send_audio produces (mono pcm_s16le), so not configurable.
CHANNELS = 1

DG_PARAMS = {
    "model": settings.deepgram_model,
    "language": settings.deepgram_language,
    "encoding": "linear16",
    "sample_rate": settings.sample_rate,
    "channels": CHANNELS,
    "interim_results": str(settings.deepgram_interim_results).lower(),
    "smart_format": str(settings.deepgram_smart_format).lower(),
    "endpointing": settings.deepgram_endpointing_ms,
    "utterance_end_ms": settings.deepgram_utterance_end_ms,
}
DG_URL = "wss://api.deepgram.com/v1/listen?" + urlencode(DG_PARAMS)


class DeepgramService:
    URL = DG_URL

    def __init__(self):
        self.current_ws = None

    def _start_ws(self):
        pass

    @property
    def headers(self):
        return {"Authorization": f"Token {settings.deepgram_api_key.get_secret_value()}"}

    def start_connection(self):
        return websockets.connect(self.URL, additional_headers=self.headers)

    @staticmethod
    async def send_audio(ws, track: rtc.Track) -> None:
        """Uplink: the SDK resamples to mono SAMPLE_RATE and hands us send_chunk_ms frames."""
        stream = rtc.AudioStream(
            track,
            sample_rate=settings.sample_rate,
            num_channels=CHANNELS,
            frame_size_ms=settings.send_chunk_ms,
        )
        try:
            async for ev in stream:
                await ws.send(bytes(ev.frame.data))  # pcm_s16le
            # Track ended on its own: have the server flush the uncommitted tail.
            await ws.send(json.dumps({"type": "CloseStream"}))
        finally:
            await stream.aclose()

    @staticmethod
    async def receive_results(
        ws, room: rtc.Room, track: rtc.Track, participant: rtc.Participant
    ) -> None:
        """Downlink: turn service messages into LiveKit text streams. Returns on done."""
        # Avoid reusing segment
        nonce = secrets.token_hex(3)

        async def publish(text: str, final: bool, turn_index) -> None:
            logger.info("Publishing to track %s, identity %s", track.sid, participant.identity)
            logger.info("Text received %s", text)
            writer = await room.local_participant.stream_text(
                topic="lk.transcription",
                sender_identity=participant.identity,
                attributes={
                    "lk.transcribed_track_id": track.sid,
                    # Prefixed with the track: Captions.tsx groups across all speakers.
                    "lk.segment_id": f"{track.sid}-{nonce}-{turn_index}",
                    "lk.transcription_final": "true" if final else "false",
                },
            )

            try:
                await writer.write(text)
            finally:
                await writer.aclose()

        turn = 0
        async for msg in ws:
            if isinstance(msg, bytes):
                continue
            data = json.loads(msg)
            logger.info(f"Received {data}")

            if data.get("type") == "Results":
                alternatives = (data.get("channel") or {}).get("alternatives") or []
                transcript = alternatives[0].get("transcript") if alternatives else ""
                is_final = data.get("is_final")

                logger.info("transcript %s, turn %s", transcript, turn)

                if transcript:
                    await publish(transcript, final=False, turn_index=turn)
                elif is_final:
                    await publish(transcript, final=True, turn_index=turn)
                    turn += 1
            else:
                logger.info("Non Result message %s", data.get("type"))
