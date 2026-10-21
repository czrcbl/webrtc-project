"""
Approach 3 - Track Egress -> WebSocket (no participant joins the room).

Flow:
  1. LiveKit sends a `track_published` webhook to POST /livekit/webhook.
  2. For each audio track we call StartTrackEgress with a websocket_url that
     points back at this server (GET /egress-audio).
  3. Egress connects to that WebSocket and streams the track as raw PCM
     (pcm_s16le, binary frames).
  4. A *mock* STT turns the audio into a fixed sentence every few seconds.
  5. We push the text into the room with the server API (RoomService.SendData)
     on topic "captions". Text streams (`lk.transcription`) need a connected
     participant, so the frontend listens for data packets instead:

        room.on(RoomEvent.DataReceived, (payload, participant, kind, topic) => {
          if (topic === "captions") console.log(JSON.parse(new TextDecoder().decode(payload)));
        });

Requirements:
  - Egress available (LiveKit Cloud, or a self-hosted egress service + Redis;
    `livekit-server --dev` alone does not run egress).
  - This server reachable from the internet (e.g. `ngrok http 8080`), and the
    webhook URL https://<public-host>/livekit/webhook set in your project settings.

Install:
    pip install livekit-api aiohttp python-dotenv

Env (.env):
    LIVEKIT_URL=wss://<project>.livekit.cloud
    LIVEKIT_API_KEY=...
    LIVEKIT_API_SECRET=...
    PUBLIC_WS_BASE=wss://<public-host>        # where egress can reach this server

Run:
    python 3_egress_transcriber.py            # listens on :8080
"""

import json
import logging
import os
import time
import uuid
from urllib.parse import urlencode

from aiohttp import WSMsgType, web
from livekit import api

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("egress-transcriber")

SEGMENT_SECONDS = 3.0
PUBLIC_WS_BASE = os.environ.get("PUBLIC_WS_BASE", "ws://localhost:8080").rstrip("/")
LIVEKIT_HTTP_URL = (
    os.environ["LIVEKIT_URL"].replace("wss://", "https://").replace("ws://", "http://")
)

print(LIVEKIT_HTTP_URL)


# --------------------------------------------------------------------------
# 1+2. Webhook: start one track egress per published audio track
# --------------------------------------------------------------------------
async def webhook(request: web.Request) -> web.Response:
    body = await request.text()
    auth = request.headers.get("Authorization", "").removeprefix("Bearer ")
    try:
        event = request.app["webhooks"].receive(body, auth)
    except Exception as e:  # bad signature / hash
        logger.warning("rejected webhook: %s", e)
        return web.Response(status=401)

    if event.event == "track_published" and event.track.type == api.TrackType.AUDIO:
        room, track, identity = (
            event.room.name,
            event.track.sid,
            event.participant.identity,
        )
        ws_url = f"{PUBLIC_WS_BASE}/egress-audio?" + urlencode(
            {"room": room, "track": track, "participant": identity}
        )
        info = await request.app["lkapi"].egress.start_track_egress(
            api.TrackEgressRequest(room_name=room, track_id=track, websocket_url=ws_url)
        )
        logger.info("egress %s started for %s/%s", info.egress_id, identity, track)

    return web.Response(text="ok")


# --------------------------------------------------------------------------
# 3+4. Egress connects here and streams raw PCM for one track
# --------------------------------------------------------------------------
async def egress_audio(request: web.Request) -> web.WebSocketResponse:
    room = request.query["room"]
    track = request.query["track"]
    participant = request.query["participant"]
    lkapi: api.LiveKitAPI = request.app["lkapi"]

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logger.info("audio websocket open for %s/%s", participant, track)

    # Mock STT state. Wall-clock based, so it doesn't depend on the PCM sample
    # rate; a real STT would get msg.data (pcm_s16le) streamed to it here.
    segment_start: float | None = None
    interim_sent = False
    n = 0
    segment_id = ""

    async for msg in ws:
        if msg.type == WSMsgType.BINARY:
            now = time.monotonic()
            if segment_start is None:
                segment_start, interim_sent, n = now, False, n + 1
                segment_id = f"SG_{uuid.uuid4().hex[:12]}"
            elapsed = now - segment_start
            text = f"Mock subtitle #{n} from {participant}"

            if not interim_sent and elapsed >= SEGMENT_SECONDS / 2:
                interim_sent = True
                await send_caption(
                    lkapi,
                    room,
                    participant,
                    track,
                    segment_id,
                    text.rsplit(" ", 2)[0] + "...",
                    False,
                )
            if elapsed >= SEGMENT_SECONDS:
                segment_start = None
                await send_caption(
                    lkapi, room, participant, track, segment_id, text, True
                )

        elif msg.type == WSMsgType.TEXT:
            logger.info(
                "egress status for %s: %s", participant, msg.data
            )  # e.g. muted/unmuted
        elif msg.type == WSMsgType.ERROR:
            logger.warning("websocket error: %s", ws.exception())

    logger.info("audio websocket closed for %s/%s", participant, track)
    return ws


# --------------------------------------------------------------------------
# 5. Push text into the room via the server API (no participant needed)
# --------------------------------------------------------------------------
async def send_caption(
    lkapi: api.LiveKitAPI,
    room: str,
    participant: str,
    track: str,
    segment_id: str,
    text: str,
    is_final: bool,
):
    payload = {
        "participant": participant,
        "track_id": track,
        "segment_id": segment_id,
        "final": is_final,
        "text": text,
    }
    await lkapi.room.send_data(
        api.SendDataRequest(
            room=room,
            data=json.dumps(payload).encode(),
            kind=api.DataPacket.Kind.RELIABLE,
            topic="captions",
        )
    )
    logger.info("%s [%s] %s", participant, "final" if is_final else "interim", text)


# --------------------------------------------------------------------------
async def on_startup(app: web.Application):
    app["lkapi"] = api.LiveKitAPI(LIVEKIT_HTTP_URL)  # key/secret from env
    app["webhooks"] = api.WebhookReceiver(api.TokenVerifier())


async def on_cleanup(app: web.Application):
    await app["lkapi"].aclose()


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/livekit/webhook", webhook)
    app.router.add_get("/egress-audio", egress_audio)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), port=int(os.environ.get("PORT", "8080")))
