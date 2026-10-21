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


settings = Settings()
