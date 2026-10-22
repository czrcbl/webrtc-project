"""
Runtime configuration, read once from environment variables (supplied by mise.toml).

Each field is read from the env var of the same name, upper-cased; matching is
case-insensitive.
"""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True, str_strip_whitespace=True)

    whisper_model: str = "small"  # tiny|base|small|medium|large-v3|large-v3-turbo|<path>
    whisper_device: Literal["auto", "cpu", "cuda"] = "auto"
    whisper_compute_type: str = "default"  # default|int8|int8_float16|float16
    whisper_workers: int = Field(2, ge=1)  # parallel transcriptions
    whisper_beam_size: int = Field(1, ge=1)  # 1 = greedy: much lower latency for streaming
    whisper_vad_min_silence_ms: int = Field(500, ge=0)  # silence that splits VAD speech chunks

    # Streaming
    decode_every_s: float = Field(1.0, gt=0)  # re-transcribe after this much new audio
    stable_margin_s: float = Field(1.0, ge=0)  # a segment this far from the buffer end is committed
    max_buffer_s: float = Field(20.0, gt=0)  # force-commit if the uncommitted audio gets this long
    prompt_chars: int = Field(200, ge=0)  # tail of committed text fed back as context


settings = Settings()
