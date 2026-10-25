"""
Listen-only LiveKit participant using the plain `livekit` rtc SDK (no Agents framework).

What it does:
  - Joins a room as a hidden participant that can subscribe and send data,
    but cannot publish audio/video.
  - For every remote audio track, streams 16 kHz mono PCM16 to an external STT
    service over a WebSocket (one socket per track).
  - Publishes what the service sends back (see src/messages.py) into the room as
    text streams on topic "lk.transcription", tied to the speaker's track:
        {"type": "partial", "text": ...}  -> lk.transcription_final = "false"
        {"type": "final", "text": ...}    -> lk.transcription_final = "true"
    A partial and the final that commits it share an lk.segment_id, so captions
    are refined in place.
"""

import asyncio
import signal
import sys

from livekit import api, rtc
from src.config import settings
from src.logger import logger
from src.services.analysis_service import AnalysisService
from src.services.executor_service import ExecutorService
from src.utils import make_token


async def bridge(
    room: rtc.Room,
    track: rtc.Track,
    participant: rtc.RemoteParticipant,
    analysis_service: AnalysisService,
) -> None:

    await ExecutorService(room, track, participant, analysis_service).run()


async def list_room_names() -> list[str]:
    lkapi = api.LiveKitAPI(
        url=settings.livekit_url,
        api_key=settings.livekit_api_key,
        api_secret=settings.livekit_api_secret.get_secret_value(),
    )
    try:
        resp = await lkapi.room.list_rooms(api.ListRoomsRequest())
        return [room.name for room in resp.rooms]
    finally:
        await lkapi.aclose()


async def main(room_name: str | None) -> None:

    room = rtc.Room()
    # Before connecting: every bridge feeds its finals into the summary inbox.
    # A bridge per track connected
    bridges: dict[str, asyncio.Task] = {}
    stop = asyncio.Event()

    def on_task_done(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error("AnalysisService failed", exc_info=task.exception())

    # Transcripts from all tracks are analyzed together
    analysis_service = AnalysisService(room)
    analysis_task = asyncio.create_task(
        analysis_service.summarize(),
    )
    analysis_task.add_done_callback(on_task_done)

    def subscribe_if_audio(pub: rtc.RemoteTrackPublication) -> None:
        if pub.kind == rtc.TrackKind.KIND_AUDIO and not pub.subscribed:
            pub.set_subscribed(True)

    def on_bridge_done(sid: str, task: asyncio.Task) -> None:
        if bridges.get(sid) is task:
            bridges.pop(sid)
        if not task.cancelled() and task.exception():
            logger.error("bridge for %s failed", sid, exc_info=task.exception())

    @room.on("track_published")
    def _on_published(pub: rtc.RemoteTrackPublication, p: rtc.RemoteParticipant):
        subscribe_if_audio(pub)

    @room.on("track_subscribed")
    def _on_subscribed(track: rtc.Track, pub: rtc.RemoteTrackPublication, p: rtc.RemoteParticipant):
        if track.kind != rtc.TrackKind.KIND_AUDIO or track.sid in bridges:
            return
        logger.info("subscribed to audio from %s (%s)", p.identity, track.sid)
        task = asyncio.create_task(bridge(room, track, p, analysis_service))
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

    if room_name is None:
        room_name = (await list_room_names())[0]

    # auto_subscribe=False: we pick the audio tracks ourselves and skip video.
    await room.connect(
        settings.livekit_url,
        make_token(room_name),
        options=rtc.RoomOptions(auto_subscribe=False),
    )
    logger.info("joined room %s as %s", room.name, settings.listener_identity)

    # Tracks published before we joined don't fire track_published.
    for p in room.remote_participants.values():
        for pub in p.track_publications.values():
            subscribe_if_audio(pub)

    # Finish
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    tasks = list(bridges.values())
    tasks.append(analysis_task)

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await room.disconnect()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else None))
