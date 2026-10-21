"""
Approach 1 - LiveKit Agents framework.

One AgentSession per remote participant. Each session runs a *mock* STT that
"recognizes" a fixed sentence every few seconds of received audio. AgentSession
publishes the results to the room on the standard `lk.transcription` text-stream
topic (interim + final, with lk.segment_id / lk.transcribed_track_id), with the
sender identity set to the participant being transcribed. No publishing code here.

Install:
    pip install livekit-agents python-dotenv

Env (.env): LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET

Run:
    python 1_agents_transcriber.py dev
"""

import asyncio
import logging

# from dotenv import load_dotenv
from livekit import rtc
from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    Agent,
    AgentServer,
    AgentSession,
    APIConnectOptions,
    AutoSubscribe,
    JobContext,
    LanguageCode,
    StopResponse,
    cli,
    llm,
    room_io,
    stt,
    utils,
)

# load_dotenv()
logger = logging.getLogger("agents-transcriber")

SEGMENT_SECONDS = 3.0  # emit one fake sentence per 3 s of received audio


# --------------------------------------------------------------------------
# Mock STT: replace this class with a real plugin, e.g.
#   inference.STT("deepgram/nova-3")  or  deepgram.STT()
# --------------------------------------------------------------------------
class MockSTT(stt.STT):
    def __init__(self, label: str = "speaker"):
        super().__init__(
            capabilities=stt.STTCapabilities(streaming=True, interim_results=True)
        )
        self._speaker = label

    async def _recognize_impl(
        self, buffer, *, language=None, conn_options=None
    ) -> stt.SpeechEvent:
        # Non-streaming path (unused by AgentSession when streaming=True)
        return _event(
            stt.SpeechEventType.FINAL_TRANSCRIPT, f"Mock subtitle from {self._speaker}"
        )

    def stream(
        self,
        *,
        language=None,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> "MockSTTStream":
        return MockSTTStream(stt=self, conn_options=conn_options, speaker=self._speaker)


class MockSTTStream(stt.RecognizeStream):
    def __init__(self, *, stt: MockSTT, conn_options: APIConnectOptions, speaker: str):
        super().__init__(stt=stt, conn_options=conn_options)
        self._speaker = speaker

    async def _run(self) -> None:
        # AgentSession pushes rtc.AudioFrame objects into self._input_ch;
        # we push SpeechEvents into self._event_ch.
        elapsed = 0.0
        n = 0
        interim_sent = False
        async for item in self._input_ch:
            if not isinstance(item, rtc.AudioFrame):
                continue  # flush sentinel

            if elapsed == 0.0:
                n += 1
                interim_sent = False
                self._event_ch.send_nowait(_event(stt.SpeechEventType.START_OF_SPEECH))

            elapsed += item.samples_per_channel / item.sample_rate
            text = f"Mock subtitle #{n} from {self._speaker}"

            if not interim_sent and elapsed >= SEGMENT_SECONDS / 2:
                interim_sent = True
                self._event_ch.send_nowait(
                    _event(
                        stt.SpeechEventType.INTERIM_TRANSCRIPT,
                        text.rsplit(" ", 2)[0] + "...",
                    )
                )

            if elapsed >= SEGMENT_SECONDS:
                elapsed = 0.0
                self._event_ch.send_nowait(
                    _event(stt.SpeechEventType.FINAL_TRANSCRIPT, text)
                )
                self._event_ch.send_nowait(_event(stt.SpeechEventType.END_OF_SPEECH))


def _event(type: stt.SpeechEventType, text: str = "") -> stt.SpeechEvent:
    alts = [stt.SpeechData(language=LanguageCode("en"), text=text)] if text else []
    return stt.SpeechEvent(type=type, alternatives=alts)


# --------------------------------------------------------------------------
# Transcribe-only agent: never replies
# --------------------------------------------------------------------------
class Transcriber(Agent):
    def __init__(self, participant_identity: str):
        super().__init__(instructions="not-needed", stt=MockSTT(participant_identity))
        self.participant_identity = participant_identity

    async def on_user_turn_completed(
        self, chat_ctx: llm.ChatContext, new_message: llm.ChatMessage
    ):
        logger.info("%s -> %s", self.participant_identity, new_message.text_content)
        raise StopResponse()  # skip LLM/TTS, transcription only


# --------------------------------------------------------------------------
# One AgentSession per participant (pattern from multi-user-transcriber.py)
# --------------------------------------------------------------------------
class MultiUserTranscriber:
    def __init__(self, ctx: JobContext):
        self.ctx = ctx
        self._sessions: dict[str, AgentSession] = {}
        self._tasks: set[asyncio.Task] = set()

    def start(self):
        self.ctx.room.on("participant_connected", self.on_participant_connected)
        self.ctx.room.on("participant_disconnected", self.on_participant_disconnected)

    def on_participant_connected(self, participant: rtc.RemoteParticipant):
        if participant.identity in self._sessions:
            return
        logger.info("starting session for %s", participant.identity)
        task = asyncio.create_task(self._start_session(participant))
        self._tasks.add(task)

        def done(t: asyncio.Task):
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is None:
                self._sessions[participant.identity] = t.result()

        task.add_done_callback(done)

    def on_participant_disconnected(self, participant: rtc.RemoteParticipant):
        session = self._sessions.pop(participant.identity, None)
        if session is None:
            return
        logger.info("closing session for %s", participant.identity)
        task = asyncio.create_task(self._close_session(session))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _start_session(self, participant: rtc.RemoteParticipant) -> AgentSession:
        session = AgentSession(
            # The mock STT emits START/END_OF_SPEECH itself, so let it end turns
            turn_handling={"turn_detection": "stt"},
        )
        await session.start(
            agent=Transcriber(participant.identity),
            room=self.ctx.room,
            room_options=room_io.RoomOptions(
                participant_identity=participant.identity,  # whose audio to listen to
                audio_input=True,
                text_output=True,  # publish transcripts on lk.transcription
                audio_output=False,
                text_input=False,
            ),
        )
        return session

    async def _close_session(self, session: AgentSession):
        await session.drain()
        await session.aclose()

    async def aclose(self):
        await utils.aio.cancel_and_wait(*self._tasks)
        await asyncio.gather(*(self._close_session(s) for s in self._sessions.values()))
        self.ctx.room.off("participant_connected", self.on_participant_connected)
        self.ctx.room.off("participant_disconnected", self.on_participant_disconnected)


server = AgentServer()


@server.rtc_session()
async def entrypoint(ctx: JobContext):
    transcriber = MultiUserTranscriber(ctx)
    transcriber.start()

    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
    for p in ctx.room.remote_participants.values():  # people already in the room
        transcriber.on_participant_connected(p)

    ctx.add_shutdown_callback(transcriber.aclose)


if __name__ == "__main__":
    cli.run_app(server)
