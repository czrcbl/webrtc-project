from os import sync
import asyncio

from livekit import rtc
from src.broadcaster import Broadcaster
from src.config import settings
from src.messages import AudioStreamMessage, TranscriptionMessage, ChatMessage
from src.services.bridge_service import BridgeService
from src.services.deepgram_service import DeepgramService
from src.services.whisper_service import WhisperService

# 50 frames of send_chunk_ms (100 ms by default) = ~5 s of audio before the
# Bridge waits on a slow STT connection.
AUDIO_QUEUE_SIZE = 50


class ExecutorService:
    """
    Runs one track's pipeline, joined only by queues of messages:
        Bridge.produce_audio -> AudioStreamMessage -> STT
        STT -> TranscriptionMessage -> Broadcaster -> Bridge.consume_transcripts
                                                 -> summary_inbox (finals only)
    Each side shuts its outgoing queue down when it is done, so the pipeline
    winds down from the track end. If any part fails, the others are cancelled.
    The summary inbox is shared by every track, so it stays open.
    """

    def __init__(
        self,
        room: rtc.Room,
        track: rtc.Track,
        participant: rtc.RemoteParticipant,
        summary_inbox: asyncio.Queue[TranscriptionMessage] | None = None,
        audio_queue_size: int = AUDIO_QUEUE_SIZE,
    ):
        self.bridge = BridgeService(room, track, participant)
        self.audio_q: asyncio.Queue[AudioStreamMessage] = asyncio.Queue(maxsize=audio_queue_size)
        self.transcripts: Broadcaster[TranscriptionMessage] = Broadcaster()
        self.transcript_q = self.transcripts.subscribe()
        if summary_inbox is not None:
            self.transcripts.subscribe(summary_inbox, accept=lambda m: m.final)
        self.comment_q: asyncio.Queue[ChatMessage] = asyncio.Queue()

        stt_cls = WhisperService if settings.stt_provider == "whisper" else DeepgramService
        self.stt = stt_cls(track.sid, participant.identity, self.audio_q, self.transcripts)

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.bridge.produce_audio(self.audio_q))
            tg.create_task(self.stt.run())
            tg.create_task(self.bridge.consume_transcripts(self.transcript_q))
