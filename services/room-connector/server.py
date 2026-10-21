# """
# Server for tapping into the call by adding a participant
# It does not use the Agents Api


# """

# from livekit.agents import AgentServer, JobContext, AutoSubscribe, cli
# from logging import Logger
# from livekit import rtc
# import asyncio
# import os
# import websockets
# import json
# import logging

# logger = logging.getLogger("transcriber")
# logger.setLevel(logging.DEBUG)

# server = AgentServer()


# STT_WS_URL = os.getenv("STT_WS_URL", "ws://localhost:8000/ws/transcribe")
# STT_LANGUAGE = os.getenv("STT_LANGUAGE", "").strip()  # "" -> auto-detect
# # STT_SAMPLE_RATE = 16_000  # what stt_server expects
# # SEND_CHUNK_MS = 100  # 3200 B per frame, the pacing stt_server/client.py uses
# SAMPLE_RATE = int(os.environ.get("WHISPER_SAMPLE_RATE", 16_000))


# @server.rtc_session(agent_name="listerner")
# async def entrypoint(ctx: JobContext):

#     tasks: dict[str, asyncio.Task] = {}

#     @ctx.room.on("track_subscribed")
#     def on_sub(track, pub, p):
#         logger.info("Subscribed to {}", track)
#         if track.kind == rtc.TrackKind.KIND_AUDIO:
#             tasks[track.sid] = asyncio.create_task(bridge(ctx.room, track, p))

#     @ctx.room.on("track_unsubscribed")
#     def on_unsub(track, pub, p):
#         logger.info("Unsubscribed to {}", track)
#         if t := tasks.pop(track.sid, None):
#             t.cancel()

#     await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)


# async def pump(ws, track):
#     async for ev in rtc.AudioStream(track, sample_rate=SAMPLE_RATE, num_channels=1):
#         await ws.send(bytes(ev.frame.data))


# async def bridge(room, track, p):
#     """Pipe the audio streams to an external service using ws"""
#     async with websockets.connect(STT_WS_URL) as ws:
#         logger.debug("Websocek connected for track {}", track)
#         await ws.send(json.dumps({"participant": p.identity, "rate": 16000}))
#         up = asyncio.create_task(pump(ws, track))
#         try:
#             async for raw in ws:  # downlink
#                 m = json.loads(raw)
#                 if m["type"] == "transcript":
#                     await room.local_participant.send_text(
#                         m["text"],
#                         topic="lk.transcription",
#                         attributes={
#                             "lk.transcribed_track_id": track.sid,
#                             "lk.segment_id": m["segment_id"],
#                             "lk.transcription_final": str(m["final"]).lower(),
#                         },
#                     )
#                 elif m["type"] == "chat":
#                     await room.local_participant.send_text(m["text"], topic="lk.chat")
#         finally:
#             up.cancel()


# if __name__ == "__main__":
#     logger.debug("Starting server")
#     cli.run_app(server)


"""
Listen-only LiveKit participant using the plain `livekit` rtc SDK (no Agents framework).
 
What it does:
  - Joins a room as a hidden participant that can subscribe and send data,
    but cannot publish audio/video.
  - For every remote audio track, streams 16 kHz mono PCM16 to an external STT
    service over a WebSocket (one socket per track).
  - Publishes what the service sends back into the room:
        {"type": "transcript", "text": ..., "segment_id": ..., "final": bool}
            -> text stream on topic "lk.transcription", tied to the speaker's track
        {"type": "chat", "text": ...}
            -> text stream on topic "lk.chat"
 
Install:
    pip install livekit livekit-api websockets
 
Run:
    LIVEKIT_URL=wss://... LIVEKIT_API_KEY=... LIVEKIT_API_SECRET=... \
    python listener.py <room-name>
"""

import asyncio
import json
import logging
import os
import signal
import sys


import websockets
from livekit import rtc, api

from src.utils import make_token
from src.models import StartMessage, TranscriptMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("listener")

LIVEKIT_URL = os.environ["LIVEKIT_URL"]
LIVEKIT_API_KEY = os.environ["LIVEKIT_API_KEY"]
LIVEKIT_API_SECRET = os.environ["LIVEKIT_API_SECRET"]

IDENTITY = os.getenv("LISTENER_IDENTITY", "listener")
STT_WS_URL = os.getenv("STT_WS_URL", "ws://localhost:8000/ws/transcribe")
SAMPLE_RATE = int(os.getenv("STT_SAMPLE_RATE", "16000"))
CHUNK_MS = int(os.getenv("SEND_CHUNK_MS", "100"))


async def send_audio(ws, track: rtc.Track) -> None:
    """Uplink: the SDK resamples to mono SAMPLE_RATE and hands us CHUNK_MS frames."""
    stream = rtc.AudioStream(
        track,
        sample_rate=SAMPLE_RATE,
        num_channels=1,
        frame_size_ms=CHUNK_MS,  # 100 ms @ 16 kHz = 3200 bytes per frame
    )
    try:
        async for ev in stream:
            await ws.send(bytes(ev.frame.data))  # pcm_s16le
    finally:
        await stream.aclose()


async def receive_results(ws, room: rtc.Room, track: rtc.Track) -> None:
    """Downlink: turn service messages into LiveKit text streams."""
    async for raw in ws:
        if isinstance(raw, bytes):
            continue  # this protocol only expects JSON text frames back
        m = json.loads(raw)
        kind = m.get("type")

        if kind == "transcript":
            await room.local_participant.send_text(
                m["text"],
                topic="lk.transcription",
                attributes={
                    "lk.transcribed_track_id": track.sid,
                    # "lk.segment_id": str(m["segment_id"]),
                    "lk.transcription_final": "true" if m.get("final") else "false",
                },
            )
        elif kind == "chat":
            await room.local_participant.send_text(m["text"], topic="lk.chat")
        else:
            logger.warning("unknown message from STT service: %s", m)


async def bridge(room: rtc.Room, track: rtc.Track, participant: rtc.RemoteParticipant) -> None:
    async with websockets.connect(STT_WS_URL) as ws:
        logger.info("STT socket open for %s (%s)", participant.identity, track.sid)
        await ws.send(
            StartMessage(participant=participant.identity, track_id=track.sid).model_dump_json(),
        )
        tasks = [
            asyncio.create_task(send_audio(ws, track)),
            asyncio.create_task(receive_results(ws, room, track)),
        ]
        try:
            # Whichever side ends first (track gone, socket closed, error) ends the bridge.
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                t.result()  # re-raise errors so they get logged
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def list_room_names() -> list[str]:
    # Reads LIVEKIT_URL / LIVEKIT_API_KEY / LIVEKIT_API_SECRET from the env.
    lkapi = api.LiveKitAPI()
    try:
        resp = await lkapi.room.list_rooms(api.ListRoomsRequest())
        return [room.name for room in resp.rooms]
    finally:
        await lkapi.aclose()


async def main(room_name: str | None) -> None:
    room = rtc.Room()
    bridges: dict[str, asyncio.Task] = {}
    stop = asyncio.Event()

    def subscribe_if_audio(pub: rtc.RemoteTrackPublication) -> None:
        if pub.kind == rtc.TrackKind.KIND_AUDIO and not pub.subscribed:
            pub.set_subscribed(True)

    def on_bridge_done(sid: str, task: asyncio.Task) -> None:
        if bridges.get(sid) is task:
            bridges.pop(sid)
        if not task.cancelled() and task.exception():
            logger.error("bridge for %s failed", sid, exc_info=task.exception())

    # Event handlers must be plain (sync) functions; spawn tasks for async work.
    @room.on("track_published")
    def _on_published(pub: rtc.RemoteTrackPublication, p: rtc.RemoteParticipant):
        subscribe_if_audio(pub)

    @room.on("track_subscribed")
    def _on_subscribed(track: rtc.Track, pub: rtc.RemoteTrackPublication, p: rtc.RemoteParticipant):
        if track.kind != rtc.TrackKind.KIND_AUDIO or track.sid in bridges:
            return
        logger.info("subscribed to audio from %s (%s)", p.identity, track.sid)
        task = asyncio.create_task(bridge(room, track, p))
        task.add_done_callback(lambda t, sid=track.sid: on_bridge_done(sid, t))
        bridges[track.sid] = task

    @room.on("track_unsubscribed")
    def _on_unsubscribed(
        track: rtc.Track, pub: rtc.RemoteTrackPublication, p: rtc.RemoteParticipant
    ):
        if task := bridges.pop(track.sid, None):
            logger.info("track %s from %s gone, closing bridge", track.sid, p.identity)
            task.cancel()

    @room.on("disconnected")
    def _on_disconnected(reason):
        logger.info("disconnected from room: %s", reason)
        stop.set()

    # auto_subscribe=False: we pick the audio tracks ourselves and skip video.
    if room_name is None:
        room_name = (await list_room_names())[0]

    await room.connect(
        LIVEKIT_URL,
        make_token(room_name),
        options=rtc.RoomOptions(auto_subscribe=False),
    )
    logger.info("joined room %s as %s", room.name, IDENTITY)

    # Tracks published before we joined don't fire track_published.
    for p in room.remote_participants.values():
        for pub in p.track_publications.values():
            subscribe_if_audio(pub)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    for task in list(bridges.values()):
        task.cancel()
    await asyncio.gather(*bridges.values(), return_exceptions=True)
    await room.disconnect()


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     sys.exit("usage: python listener.py <room-name>")
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else None))
