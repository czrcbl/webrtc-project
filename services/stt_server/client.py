"""
Test client: streams a file (or the microphone) to the server in real time.

    python client.py audio.mp3                 # any format ffmpeg can read
    python client.py audio.mp3 --fast          # send as fast as possible
    python client.py --mic                     # PulseAudio/PipeWire default mic, Ctrl+C to stop
    python client.py talk.wav --language pt --url ws://myserver:8000/ws/transcribe
"""

import argparse
import asyncio
import json
import signal
import sys

from websockets.asyncio.client import connect

SAMPLE_RATE = 16_000
CHUNK_S = 0.1
CHUNK_BYTES = int(SAMPLE_RATE * CHUNK_S) * 2  # 16-bit mono


async def ffmpeg_pcm(source: list[str]):
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-loglevel", "error", *source,
        "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "-",
        stdout=asyncio.subprocess.PIPE,
    )
    return proc


async def send_audio(ws, proc, realtime: bool):
    try:
        while True:
            chunk = await proc.stdout.read(CHUNK_BYTES)
            if not chunk:
                break
            await ws.send(chunk)
            if realtime:
                await asyncio.sleep(CHUNK_S)
    finally:
        await ws.send(json.dumps({"type": "eof"}))


async def receive(ws):
    async for raw in ws:
        msg = json.loads(raw)
        kind = msg["type"]
        if kind == "partial":
            sys.stdout.write(f"\r\033[K… {msg['text']}")
            sys.stdout.flush()
        elif kind == "final":
            sys.stdout.write(f"\r\033[K[{msg['start']:7.2f} → {msg['end']:7.2f}] {msg['text']}\n")
        elif kind == "done":
            print(f"\r\033[K-- done (language: {msg.get('language')})")
            return
        elif kind == "error":
            print(f"\nerror: {msg['message']}", file=sys.stderr)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("file", nargs="?")
    p.add_argument("--mic", action="store_true")
    p.add_argument("--fast", action="store_true", help="don't pace to real time")
    p.add_argument("--language")
    p.add_argument("--url", default="ws://localhost:8000/ws/transcribe")
    a = p.parse_args()

    if a.mic:
        source, realtime = ["-f", "pulse", "-i", "default"], False  # mic is already real time
    elif a.file:
        source, realtime = ["-i", a.file], not a.fast
    else:
        p.error("give a file or --mic")

    url = a.url + (f"?language={a.language}" if a.language else "")
    async with connect(url, max_size=None) as ws:
        assert json.loads(await ws.recv())["type"] == "ready"
        proc = await ffmpeg_pcm(source)
        # Ctrl+C stops ffmpeg; the sender then sees EOF and asks the server to
        # flush, so the last words aren't lost.
        asyncio.get_running_loop().add_signal_handler(signal.SIGINT, proc.terminate)
        sender = asyncio.create_task(send_audio(ws, proc, realtime))
        try:
            await receive(ws)
        finally:
            sender.cancel()
            if proc.returncode is None:
                proc.terminate()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
