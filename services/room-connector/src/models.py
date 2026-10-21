from typing import Literal

from pydantic import BaseModel
from src.messages import SAMPLE_RATE


class TranscriptMessage(BaseModel):
    type: Literal["final", "transcript"]
    text: str


class StartMessage(BaseModel):
    participant: str
    track_id: str
    sample_rate: int = SAMPLE_RATE
    channels: int = 1
    encoding: str = "pcm_s16le"
