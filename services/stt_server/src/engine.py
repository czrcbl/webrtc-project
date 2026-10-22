"""faster-whisper behind a thread pool, so blocking transcriptions don't stall the event loop."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from faster_whisper import WhisperModel
from pydantic import BaseModel

from .config import settings

log = logging.getLogger("stt")


class Segment(BaseModel):
    """Times are seconds from the start of the audio that was transcribed."""

    start: float
    end: float
    text: str


class WhisperEngine:
    def __init__(self) -> None:
        log.info(
            "Loading %s on %s (%s)",
            settings.whisper_model,
            settings.whisper_device,
            settings.whisper_compute_type,
        )
        self.model = WhisperModel(
            settings.whisper_model,
            device=settings.whisper_device,
            compute_type=settings.whisper_compute_type,
            num_workers=settings.whisper_workers,
        )
        # transcribe() is blocking; run it in a pool sized to the model's workers so
        # extra connections queue instead of oversubscribing the CPU/GPU.
        self.pool = ThreadPoolExecutor(max_workers=settings.whisper_workers)

    def close(self) -> None:
        self.pool.shutdown(wait=False)

    async def transcribe(
        self, audio: np.ndarray, language: str | None, task: str, prompt: str | None
    ) -> list[Segment]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.pool, self._transcribe_sync, audio, language, task, prompt
        )

    def _transcribe_sync(
        self, audio: np.ndarray, language: str | None, task: str, prompt: str | None
    ) -> list[Segment]:
        """Runs in a worker thread. Consumes the segment generator there, too."""
        segments, _ = self.model.transcribe(
            audio,
            language=language,
            task=task,
            beam_size=settings.whisper_beam_size,
            vad_filter=True,  # skip silence, fewer hallucinations
            vad_parameters={"min_silence_duration_ms": settings.whisper_vad_min_silence_ms},
            condition_on_previous_text=False,  # we pass our own prompt instead
            initial_prompt=prompt or None,
        )
        return [
            Segment(start=float(s.start), end=float(s.end), text=s.text.strip()) for s in segments
        ]
