"""
Prototype -> LiveKit Agents framework.

One AgentSession per remote participant. Each session runs a streaming STT that
bridges to the local faster-whisper server in `stt_server/` over a WebSocket.
AgentSession publishes the results to the room on the standard `lk.transcription`
text-stream topic (interim + final, with lk.segment_id / lk.transcribed_track_id),
with the sender identity set to the participant being transcribed. No publishing
code here.

Env (supplied by mise.toml):
    LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET
    STT_WS_URL    WebSocket endpoint of stt_server
    STT_LANGUAGE  ISO code to pin the language; empty means auto-detect

Run:
    mise run transcriber        # or: python room-connector/agent.py dev
"""

import asyncio
import json
import logging
import os
from urllib.parse import urlencode

from livekit import rtc
from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    Agent,
    AgentServer,
    AgentSession,
    APIConnectionError,
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
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

logger = logging.getLogger("agents-transcriber")

STT_WS_URL = os.getenv("STT_WS_URL", "ws://localhost:8000/ws/transcribe")
STT_LANGUAGE = os.getenv("STT_LANGUAGE", "").strip()  # "" -> auto-detect
STT_SAMPLE_RATE = 16_000  # what stt_server expects
SEND_CHUNK_MS = 100  # 3200 B per frame, the pacing stt_server/client.py uses


class WhisperSTT(stt.STT):
    def __init__(self) -> None:
        super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=True))

    async def _recognize_impl(self, buffer, *, language=None, conn_options=None) -> stt.SpeechEvent:
        # Capabilities declare streaming=True, so AgentSession always uses
        # stream(); stt_server has no one-shot endpoint to call here anyway.
        raise NotImplementedError("WhisperSTT is streaming-only")

    def stream(
        self,
        *,
        language=None,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> WhisperSTTStream:
        return WhisperSTTStream(
            stt=self,
            conn_options=conn_options,
            language=language or STT_LANGUAGE or None,
        )


class WhisperSTTStream(stt.RecognizeStream):
    def __init__(
        self,
        *,
        stt: WhisperSTT,
        conn_options: APIConnectOptions,
        language: str | None,
    ):
        # Passing sample_rate makes the base class resample the room audio
        # (mono 24 kHz) down to the 16 kHz stt_server expects.
        super().__init__(stt=stt, conn_options=conn_options, sample_rate=STT_SAMPLE_RATE)
        self._language = language

    def _url(self) -> str:
        q = {"task": "transcribe"}
        if self._language:
            q["language"] = self._language
        return f"{STT_WS_URL}?{urlencode(q)}"

    async def _run(self) -> None:
        try:
            async with connect(self._url(), max_size=None) as ws:
                hello = json.loads(await ws.recv())
                if hello.get("type") != "ready":
                    raise APIConnectionError(f"bad handshake: {hello}")
                logger.debug("connected to %s", STT_WS_URL)
                await asyncio.gather(self._send(ws), self._recv(ws))
        except WebSocketException as e:
            # Retryable: RecognizeStream._main_task backs off and re-runs us.
            raise APIConnectionError("stt_server connection lost") from e

    async def _send(self, ws) -> None:
        """Forward PCM to the server, batched to SEND_CHUNK_MS per frame."""
        bstream = utils.audio.AudioByteStream(
            sample_rate=STT_SAMPLE_RATE,
            num_channels=1,
            samples_per_channel=STT_SAMPLE_RATE * SEND_CHUNK_MS // 1000,
        )
        async for item in self._input_ch:
            # Flush sentinels are deliberately ignored. {"type": "eof"} makes the
            # server flush, report `done` and close, but a flush only marks a turn
            # boundary; stt_server does its own VAD segmentation, so one
            # connection covers the whole session.
            if not isinstance(item, rtc.AudioFrame):
                continue
            for chunk in bstream.write(item.data.tobytes()):
                await ws.send(chunk.data.tobytes())

        for chunk in bstream.flush():
            await ws.send(chunk.data.tobytes())
        await ws.send(json.dumps({"type": "eof"}))

    async def _recv(self, ws) -> None:
        """Map the stt_server protocol onto SpeechEvents."""
        speaking = False
        async for raw in ws:
            if isinstance(raw, bytes):
                continue
            msg = json.loads(raw)
            kind = msg.get("type")
            # The server emits a partial on every decode, blank ones included, so
            # empty text is dropped rather than published as an empty caption.
            text = (msg.get("text") or "").strip()

            if kind == "partial" and text:
                if not speaking:
                    speaking = True
                    self._event_ch.send_nowait(_event(stt.SpeechEventType.START_OF_SPEECH))
                self._event_ch.send_nowait(_event(stt.SpeechEventType.INTERIM_TRANSCRIPT, text))
            elif kind == "final" and text:
                if not speaking:
                    self._event_ch.send_nowait(_event(stt.SpeechEventType.START_OF_SPEECH))
                self._event_ch.send_nowait(
                    _event(
                        stt.SpeechEventType.FINAL_TRANSCRIPT,
                        text,
                        start=msg.get("start", 0.0),
                        end=msg.get("end", 0.0),
                    )
                )
                self._event_ch.send_nowait(_event(stt.SpeechEventType.END_OF_SPEECH))
                speaking = False
            elif kind == "done":
                return
            elif kind == "error":
                raise APIConnectionError(f"stt_server: {msg.get('message')}")


def _event(
    type: stt.SpeechEventType,
    text: str = "",
    *,
    start: float = 0.0,
    end: float = 0.0,
) -> stt.SpeechEvent:
    # stt_server only reports the detected language in its closing `done`
    # message, too late to tag segments with, so fall back to "en".
    alts = (
        [
            stt.SpeechData(
                language=LanguageCode(STT_LANGUAGE or "en"),
                text=text,
                start_time=start,
                end_time=end,
            )
        ]
        if text
        else []
    )
    return stt.SpeechEvent(type=type, alternatives=alts)


class Transcriber(Agent):
    def __init__(self, participant_identity: str):
        super().__init__(instructions="not-needed", stt=WhisperSTT())
        self.participant_identity = participant_identity

    async def on_user_turn_completed(self, chat_ctx: llm.ChatContext, new_message: llm.ChatMessage):
        logger.info("%s -> %s", self.participant_identity, new_message.text_content)
        raise StopResponse()  # skip LLM/TTS, transcription only


class MultiUserTranscriber:
    def __init__(self, ctx: JobContext):
        self.ctx = ctx
        self._sessions: dict[str, AgentSession] = {}
        self._starting: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    def start(self):
        self.ctx.room.on("participant_connected", self.on_participant_connected)
        self.ctx.room.on("participant_disconnected", self.on_participant_disconnected)

    def on_participant_connected(self, participant: rtc.RemoteParticipant):
        identity = participant.identity
        # _sessions is only filled from the done callback below, so someone
        # already in the room when entrypoint() runs would otherwise be started
        # twice: once by the event, once by the catch-up loop.
        if identity in self._sessions or identity in self._starting:
            return
        self._starting.add(identity)
        logger.info("starting session for %s", identity)
        task = asyncio.create_task(self._start_session(participant))
        self._tasks.add(task)

        def done(t: asyncio.Task):
            self._tasks.discard(t)
            self._starting.discard(identity)
            if not t.cancelled() and t.exception() is None:
                self._sessions[identity] = t.result()

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
            # WhisperSTTStream emits START/END_OF_SPEECH itself, so let it end
            # turns. This also avoids the default inference.TurnDetector(),
            # which would call LiveKit Cloud.
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


# Entrypoint for the process dispatched when a new Room is created
@server.rtc_session()
async def entrypoint(ctx: JobContext):
    transcriber = MultiUserTranscriber(ctx)
    transcriber.start()

    logger.info("Dispatching agent process context {}", JobContext)

    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
    for p in ctx.room.remote_participants.values():  # people already in the room
        transcriber.on_participant_connected(p)

    ctx.add_shutdown_callback(transcriber.aclose)


if __name__ == "__main__":
    cli.run_app(server)
