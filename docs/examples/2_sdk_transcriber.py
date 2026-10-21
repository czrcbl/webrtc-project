"""
Approach 2 - plain LiveKit Python SDK (no Agents framework).

Joins the room as a normal participant, reads every remote audio track with
rtc.AudioStream, runs a *mock* STT on it, and publishes the result on the
standard `lk.transcription` text-stream topic using the same attributes the
Agents framework uses, so any LiveKit frontend (useTranscriptions,
registerTextStreamHandler("lk.transcription")) shows it as subtitles.

Install:
    pip install livekit livekit-api python-dotenv

Env (.env): LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET

Run:
    python 2_sdk_transcriber.py my-room
"""

import asyncio
import logging
import os
import signal
import sys
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

from dotenv import load_dotenv
from livekit import api, rtc

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sdk-transcriber")

SEGMENT_SECONDS = 3.0


# --------------------------------------------------------------------------
# Mock STT: swap for your provider's streaming client (Deepgram, AssemblyAI...)
# --------------------------------------------------------------------------
@dataclass
class Transcript:
    text: str
    is_final: bool
    segment_id: str


async def mock_stt(audio: AsyncIterator[rtc.AudioFrameEvent], speaker: str) -> AsyncIterator[Transcript]:
    """Every SEGMENT_SECONDS of audio: one interim result, then a final one."""
    elapsed, n, interim_sent = 0.0, 0, False
    segment_id = ""
    async for event in audio:
        frame = event.frame
        if elapsed == 0.0:
            n += 1
            interim_sent = False
            segment_id = f"SG_{uuid.uuid4().hex[:12]}"

        elapsed += frame.samples_per_channel / frame.sample_rate
        text = f"Mock subtitle #{n} from {speaker}"

        if not interim_sent and elapsed >= SEGMENT_SECONDS / 2:
            interim_sent = True
            yield Transcript(text.rsplit(" ", 2)[0] + "...", False, segment_id)

        if elapsed >= SEGMENT_SECONDS:
            elapsed = 0.0
            yield Transcript(text, True, segment_id)


# --------------------------------------------------------------------------
# Publishing on lk.transcription (what AgentSession does for you)
# --------------------------------------------------------------------------
async def publish_transcript(
    room: rtc.Room, participant: rtc.RemoteParticipant, track_sid: str, t: Transcript
):
    writer = await room.local_participant.stream_text(
        topic="lk.transcription",
        attributes={
            "lk.transcribed_track_id": track_sid,
            "lk.segment_id": t.segment_id,
            "lk.transcription_final": "true" if t.is_final else "false",
        },
        # Make the stream appear to come from the speaker, like Agents does.
        sender_identity=participant.identity,
    )
    await writer.write(t.text)
    await writer.aclose()


async def transcribe_track(
    room: rtc.Room, track: rtc.RemoteAudioTrack, participant: rtc.RemoteParticipant
):
    stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1)
    try:
        async for t in mock_stt(stream, participant.identity):
            logger.info("%s [%s] %s", participant.identity, "final" if t.is_final else "interim", t.text)
            await publish_transcript(room, participant, track.sid, t)
    finally:
        await stream.aclose()


# --------------------------------------------------------------------------
# Room plumbing
# --------------------------------------------------------------------------
def make_token(room_name: str) -> str:
    return (
        api.AccessToken()  # reads LIVEKIT_API_KEY / LIVEKIT_API_SECRET
        .with_identity("sdk-transcriber")
        .with_name("Transcriber")
        .with_grants(
            api.VideoGrants(
                room_join=True,
                room=room_name,
                can_subscribe=True,
                can_publish=False,      # no media
                can_publish_data=True,  # needed for text streams
            )
        )
        .to_jwt()
    )


async def main(room_name: str):
    room = rtc.Room()
    tasks: dict[str, asyncio.Task] = {}
    stop = asyncio.Event()

    @room.on("track_subscribed")
    def on_track_subscribed(track: rtc.Track, pub: rtc.RemoteTrackPublication, p: rtc.RemoteParticipant):
        if track.kind != rtc.TrackKind.KIND_AUDIO or pub.sid in tasks:
            return
        logger.info("transcribing %s (%s)", p.identity, pub.sid)
        task = asyncio.create_task(transcribe_track(room, track, p))
        tasks[pub.sid] = task
        task.add_done_callback(lambda _t, s=pub.sid: tasks.pop(s, None))

    @room.on("track_unsubscribed")
    def on_track_unsubscribed(track, pub: rtc.RemoteTrackPublication, p):
        if task := tasks.pop(pub.sid, None):
            task.cancel()

    @room.on("disconnected")
    def on_disconnected(reason):
        logger.info("disconnected: %s", reason)
        stop.set()

    await room.connect(os.environ["LIVEKIT_URL"], make_token(room_name), options=rtc.RoomOptions(auto_subscribe=True))
    logger.info("connected to %s", room.name)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    pending = list(tasks.values())
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await room.disconnect()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "my-room"))
