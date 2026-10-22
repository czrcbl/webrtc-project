"""
Runtime configuration, read once from environment variables (supplied by mise.toml).

Each field is read from the env var of the same name, upper-cased; matching is
case-insensitive. Missing required vars fail at import with a validation error
that names them.
"""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True, str_strip_whitespace=True)

    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: SecretStr

    listener_identity: str = "listener"
    stt_ws_url: str = "ws://localhost:8000/ws/transcribe"
    stt_language: str = ""  # "" -> auto-detect
    send_chunk_ms: int = Field(100, gt=0)  # 100 ms @ 16 kHz = 3200 bytes per frame
    sample_rate: int = 16_000

    deepgram_api_key: SecretStr
    deepgram_model: str = "nova-3"
    deepgram_language: str = "pt-BR"  # e.g. "pt-BR", or "multi" for code-switching
    deepgram_interim_results: bool = True  # partial hypotheses while the person is still talking
    deepgram_smart_format: bool = True  # punctuation, numbers, etc.
    deepgram_endpointing_ms: int = Field(300, ge=10)  # silence before Deepgram marks speech_final
    deepgram_utterance_end_ms: int = Field(1000, ge=1000)  # word gap before UtteranceEnd


settings = Settings()
