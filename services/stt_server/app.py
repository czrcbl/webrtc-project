"""
Streaming speech-to-text over WebSocket, backed by faster-whisper.

Run (from the repo root; the imports are package-relative):
    mise run stt-server
    # or: uvicorn services.stt_server.app:app --host 0.0.0.0 --port 8000

Protocol (one WebSocket connection = one audio stream; details in src/messages.py):
    Connect:  ws://HOST:8000/ws/transcribe?language=pt&task=transcribe
              (all parameters in the query string, no handshake)
    Client -> server:
        binary frames   raw PCM, signed 16-bit little-endian, mono, 16 kHz
        text frame      eof   flush remaining audio and finish
    Server -> client (JSON text frames):
        {"text": "...", "final": false}   may still change
        {"text": "...", "final": true}    stable
    After eof the server closes with 1000; bad parameters close with 1008 and
    server failures with 1011, with the reason in the close frame.

Configuration (environment variables, parsed in src/config.py):
    WHISPER_MODEL         tiny|base|small|medium|large-v3|large-v3-turbo|<path>  (default: small)
    WHISPER_DEVICE        auto|cpu|cuda                     (default: auto)
    WHISPER_COMPUTE_TYPE  default|int8|int8_float16|float16 (default: default)
    WHISPER_WORKERS       parallel transcriptions           (default: 2)
    plus the decoding and streaming settings listed there.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from .src.config import settings
from .src.engine import WhisperEngine
from .src.messages import EOF, StreamParams, TranscriptMessage
from .src.stream import Stream

log = logging.getLogger("stt")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.engine = WhisperEngine()
    yield
    app.state.engine.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "model": settings.whisper_model}


async def send(ws: WebSocket, messages: list[TranscriptMessage]) -> None:
    for msg in messages:
        await ws.send_text(msg.model_dump_json())


async def close(ws: WebSocket, code: int, reason: str) -> None:
    # Close reasons are capped at 123 bytes by the WebSocket spec.
    await ws.close(code=code, reason=reason.encode()[:123].decode(errors="ignore"))


@app.websocket("/ws/transcribe")
async def ws_transcribe(ws: WebSocket):
    await ws.accept()
    try:
        params = StreamParams.model_validate(dict(ws.query_params))
    except ValidationError as e:
        reason = "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors())
        await close(ws, 1008, reason)
        return

    log.info(
        "stream open: participant=%s track=%s language=%s task=%s",
        params.participant,
        params.track_id,
        params.language,
        params.task,
    )
    st = Stream(engine=ws.app.state.engine, language=params.language, task=params.task)

    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                return

            if msg.get("bytes") is not None:
                st.add_pcm16(msg["bytes"])
                if st.decode_due:
                    await send(ws, await st.decode(final=False))

            elif msg.get("text") is not None:
                if msg["text"].strip() != EOF:
                    log.warning("ignoring unexpected text frame: %.50s", msg["text"])
                    continue
                await send(ws, await st.decode(final=True))
                await ws.close()
                return

    except WebSocketDisconnect:
        log.info("client disconnected")
    except Exception as e:  # keep one bad stream from killing the server
        log.exception("stream failed")
        try:
            await close(ws, 1011, str(e))
        except Exception:
            pass
