"""
Streaming speech-to-text over WebSocket, backed by faster-whisper.

Run (from the repo root; the imports are package-relative):
    mise run stt-server
    # or: uvicorn services.stt_server.server:app --host 0.0.0.0 --port 8000

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
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from faster_whisper import WhisperModel
from pydantic import ValidationError

from .src.config import settings
from .src.messages import EOF, SAMPLE_RATE, StreamParams, TranscriptMessage

log = logging.getLogger("stt")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Streaming knobs
DECODE_EVERY_S = 1.0  # re-transcribe after this much new audio
STABLE_MARGIN_S = 1.0  # a segment this far from the buffer end can be committed
MAX_BUFFER_S = 20.0  # force-commit if the uncommitted audio gets this long
PROMPT_CHARS = 200  # tail of committed text fed back as context

state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "Loading %s on %s (%s)",
        settings.whisper_model,
        settings.whisper_device,
        settings.whisper_compute_type,
    )
    state["model"] = WhisperModel(
        settings.whisper_model,
        device=settings.whisper_device,
        compute_type=settings.whisper_compute_type,
        num_workers=settings.whisper_workers,
    )
    # transcribe() is blocking; run it in a pool sized to the model's workers so
    # extra connections queue instead of oversubscribing the CPU/GPU.
    state["pool"] = ThreadPoolExecutor(max_workers=settings.whisper_workers)
    yield
    state["pool"].shutdown(wait=False)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "model": settings.whisper_model}


@dataclass
class Segment:
    start: float
    end: float
    text: str


def transcribe_sync(audio: np.ndarray, language: str | None, task: str, prompt: str | None):
    """Runs in a worker thread. Consumes the segment generator there, too."""
    segments, info = state["model"].transcribe(
        audio,
        language=language,
        task=task,
        beam_size=1,  # greedy: much lower latency for streaming
        vad_filter=True,  # skip silence, fewer hallucinations
        vad_parameters={"min_silence_duration_ms": 500},
        condition_on_previous_text=False,  # we pass our own prompt instead
        initial_prompt=prompt or None,
    )
    return [Segment(float(s.start), float(s.end), s.text.strip()) for s in segments], info.language


@dataclass
class Stream:
    language: str | None
    task: str
    buffer: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.float32))
    offset_s: float = 0.0  # absolute time of buffer[0] within the stream
    pending_new: int = 0  # samples received since the last decode
    carry: bytes = b""  # odd trailing byte from a split sample
    committed: list[str] = field(default_factory=list)

    def add_pcm16(self, data: bytes) -> None:
        data = self.carry + data
        if len(data) % 2:
            data, self.carry = data[:-1], data[-1:]
        else:
            self.carry = b""
        samples = np.frombuffer(data, dtype="<i2").astype(np.float32) / 32768.0
        self.buffer = np.concatenate([self.buffer, samples])
        self.pending_new += len(samples)

    @property
    def buffer_s(self) -> float:
        return len(self.buffer) / SAMPLE_RATE

    def prompt(self) -> str:
        return " ".join(self.committed)[-PROMPT_CHARS:]


async def send(ws: WebSocket, text: str, final: bool) -> None:
    await ws.send_text(TranscriptMessage(text=text, final=final).model_dump_json())


async def close(ws: WebSocket, code: int, reason: str) -> None:
    # Close reasons are capped at 123 bytes by the WebSocket spec.
    await ws.close(code=code, reason=reason.encode()[:123].decode(errors="ignore"))


async def decode(ws: WebSocket, st: Stream, final: bool) -> None:
    st.pending_new = 0
    if len(st.buffer) == 0:
        return

    loop = asyncio.get_running_loop()
    segs, _ = await loop.run_in_executor(
        state["pool"], transcribe_sync, st.buffer.copy(), st.language, st.task, st.prompt()
    )
    length = st.buffer_s

    if not segs:
        # Only silence so far. Keep a short tail so a word that is just
        # starting isn't cut, and drop the rest to keep decodes cheap.
        if not final and length > 5.0:
            keep = int(0.5 * SAMPLE_RATE)
            st.offset_s += (len(st.buffer) - keep) / SAMPLE_RATE
            st.buffer = st.buffer[-keep:]
        return

    if final:
        n_commit = len(segs)
    else:
        # Commit segments that are followed by another one and end well before
        # the buffer edge; Whisper rarely revises those.
        n_commit = 0
        for i, s in enumerate(segs[:-1]):
            if s.end <= length - STABLE_MARGIN_S:
                n_commit = i + 1
        # Long monologue with no break: commit everything but the last segment,
        # or the whole thing if it's a single segment.
        if n_commit == 0 and length >= MAX_BUFFER_S:
            n_commit = max(len(segs) - 1, 1)

    for s in segs[:n_commit]:
        if s.text:
            st.committed.append(s.text)
            await send(ws, s.text, final=True)

    if n_commit:
        cut_s = segs[n_commit - 1].end
        cut = min(int(cut_s * SAMPLE_RATE), len(st.buffer))
        st.buffer = st.buffer[cut:]
        st.offset_s += cut / SAMPLE_RATE

    partial = " ".join(s.text for s in segs[n_commit:] if s.text)
    if partial and not final:
        await send(ws, partial, final=False)


@app.websocket("/ws/transcribe")
async def ws_transcribe(ws: WebSocket):
    await ws.accept()
    try:
        params = StreamParams.model_validate(dict(ws.query_params))
    except ValidationError as e:
        await close(
            ws,
            1008,
            "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()),
        )
        return

    log.info(
        "stream open: participant=%s track=%s language=%s task=%s",
        params.participant,
        params.track_id,
        params.language,
        params.task,
    )
    st = Stream(language=params.language, task=params.task)

    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                return

            if msg.get("bytes") is not None:
                st.add_pcm16(msg["bytes"])
                if st.pending_new >= DECODE_EVERY_S * SAMPLE_RATE:
                    await decode(ws, st, final=False)

            elif msg.get("text") is not None:
                if msg["text"].strip() != EOF:
                    log.warning("ignoring unexpected text frame: %.50s", msg["text"])
                    continue
                await decode(ws, st, final=True)
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
