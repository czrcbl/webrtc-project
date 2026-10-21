"""
Test helper for all three transcribers.

Joins the room as N fake speakers (each publishes a sine-wave mic track) plus
one silent "viewer" that prints every subtitle it receives:
  - `lk.transcription` text streams   (approaches 1 and 2)
  - data packets on topic "captions"  (approach 3)

Install:
    pip install livekit livekit-api python-dotenv

Run (in another terminal, after starting a transcriber):
    python test_room.py my-room --speakers 2
"""

import argparse
import array
import asyncio
import json
import math
import os

from livekit import api, rtc

SAMPLE_RATE = 48000
FRAME_MS = 10


def token(room: str, identity: str) -> str:
    return (
        api.AccessToken()
        .with_identity(identity)
        .with_grants(api.VideoGrants(room_join=True, room=room))
        .to_jwt()
    )


async def speaker(room_name: str, identity: str, freq: float, stop: asyncio.Event):
    room = rtc.Room()
    await room.connect(os.environ["LIVEKIT_URL"], token(room_name, identity))

    source = rtc.AudioSource(SAMPLE_RATE, 1)
    track = rtc.LocalAudioTrack.create_audio_track("mic", source)
    await room.local_participant.publish_track(
        track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
    )
    print(f"[{identity}] publishing a {freq:.0f} Hz tone")

    spc = SAMPLE_RATE * FRAME_MS // 1000  # samples per 10 ms frame
    phase = 0
    try:
        while not stop.is_set():
            samples = array.array(
                "h",
                (int(8000 * math.sin(2 * math.pi * freq * (phase + i) / SAMPLE_RATE)) for i in range(spc)),
            )
            phase += spc
            frame = rtc.AudioFrame(samples.tobytes(), SAMPLE_RATE, 1, spc)
            await source.capture_frame(frame)  # paces itself in real time
    finally:
        await room.disconnect()


async def viewer(room_name: str, stop: asyncio.Event):
    room = rtc.Room()

    def on_transcription(reader: rtc.TextStreamReader, sender_identity: str):
        async def read():
            text = await reader.read_all()
            attrs = reader.info.attributes or {}
            kind = "final  " if attrs.get("lk.transcription_final") == "true" else "interim"
            print(f"lk.transcription {kind} | {sender_identity:>10} | {text}")

        asyncio.create_task(read())

    room.register_text_stream_handler("lk.transcription", on_transcription)

    @room.on("data_received")
    def on_data(packet: rtc.DataPacket):
        if packet.topic == "captions":
            c = json.loads(packet.data)
            kind = "final  " if c["final"] else "interim"
            print(f"captions         {kind} | {c['participant']:>10} | {c['text']}")

    await room.connect(os.environ["LIVEKIT_URL"], token(room_name, "viewer"))
    print("[viewer] waiting for subtitles... (Ctrl+C to stop)")
    await stop.wait()
    await room.disconnect()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("room", nargs="?", default="my-room")
    parser.add_argument("--speakers", type=int, default=2)
    args = parser.parse_args()

    stop = asyncio.Event()
    jobs = [viewer(args.room, stop)] + [
        speaker(args.room, f"speaker-{i + 1}", 220 * (i + 1), stop) for i in range(args.speakers)
    ]
    try:
        await asyncio.gather(*jobs)
    except asyncio.CancelledError:
        stop.set()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
