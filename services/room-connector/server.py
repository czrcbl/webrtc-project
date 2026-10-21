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
 
Install:
    pip install livekit livekit-api websockets
 
Run:
    LIVEKIT_URL=wss://... LIVEKIT_API_KEY=... LIVEKIT_API_SECRET=... \
    python listener.py <room-name>
"""

import asyncio
import logging
import signal
import sys
from urllib.parse import urlencode

import websockets
from livekit import api, rtc
from src.config import settings
from src.messages import (
    SAMPLE_RATE,
    DoneMessage,
    EofMessage,
    ErrorMessage,
    FinalMessage,
    PartialMessage,
    ReadyMessage,
    server_message,
)
from src.utils import make_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("listener")


async def send_audio(ws, track: rtc.Track) -> None:
    """Uplink: the SDK resamples to mono SAMPLE_RATE and hands us send_chunk_ms frames."""
    stream = rtc.AudioStream(
        track,
        sample_rate=SAMPLE_RATE,
        num_channels=1,
        frame_size_ms=settings.send_chunk_ms,
    )
    try:
        async for ev in stream:
            await ws.send(bytes(ev.frame.data))  # pcm_s16le
        # Track ended on its own: have the server flush the uncommitted tail.
        await ws.send(EofMessage().model_dump_json())
    finally:
        await stream.aclose()


async def receive_results(ws, room: rtc.Room, track: rtc.Track) -> None:
    """Downlink: turn service messages into LiveKit text streams. Returns on done."""
    seq = 0  # partials share the open segment's id; each final closes it

    async def publish(text: str, final: bool) -> None:
        await room.local_participant.send_text(
            text,
            topic="lk.transcription",
            attributes={
                "lk.transcribed_track_id": track.sid,
                # Prefixed with the track: Captions.tsx groups across all speakers.
                "lk.segment_id": f"{track.sid}-{seq}",
                "lk.transcription_final": "true" if final else "false",
            },
        )

    async for raw in ws:
        if isinstance(raw, bytes):
            continue  # this protocol only expects JSON text frames back
        match server_message.validate_json(raw):
            case PartialMessage(text=text) if text:
                await publish(text, final=False)
            case FinalMessage(text=text):
                await publish(text, final=True)
                seq += 1
            case ErrorMessage(message=message):
                logger.error("STT error on %s: %s", track.sid, message)
            case DoneMessage(language=language):
                logger.info("STT done for %s (language: %s)", track.sid, language)
                return
            case _:
                pass  # empty partial: nothing uncommitted left


async def bridge(room: rtc.Room, track: rtc.Track, participant: rtc.RemoteParticipant) -> None:
    # The server takes its options from the query string; there is no start frame.
    url = settings.stt_ws_url
    if settings.stt_language:
        url += f"?{urlencode({'language': settings.stt_language})}"
    async with websockets.connect(url) as ws:
        hello = server_message.validate_json(await ws.recv())
        if not isinstance(hello, ReadyMessage):
            raise RuntimeError(f"STT server did not send ready: {hello}")
        logger.info("STT socket open for %s (%s)", participant.identity, track.sid)

        tasks = [
            asyncio.create_task(send_audio(ws, track)),
            asyncio.create_task(receive_results(ws, room, track)),
        ]
        try:
            # Audio ending only sends eof; the bridge ends when the results side
            # finishes (done or socket closed), or as soon as either side raises.
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                t.result()  # re-raise errors so they get logged
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


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
        settings.livekit_url,
        make_token(room_name),
        options=rtc.RoomOptions(auto_subscribe=False),
    )
    logger.info("joined room %s as %s", room.name, settings.listener_identity)

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
