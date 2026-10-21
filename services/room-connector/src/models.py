from pydantic import BaseModel
from typing import Literal
import os

# TODO: create config
SAMPLE_RATE = int(os.getenv("STT_SAMPLE_RATE", "16000"))


class TranscriptMessage(BaseModel):
    type: Literal["final", "transcript"]
    text: str


class StartMessage(BaseModel):
    participant: str
    track_id: str
    sample_rate: int = SAMPLE_RATE
    channels: int = 1
    encoding: str = "pcm_s16le"
