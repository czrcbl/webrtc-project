"""
Create a LiveKit room, mint a join token and open the Meet client on it.

Run (with the livekit server and `mise run meet-dev` already up):
    python room_manager.py                 # uses $ROOM / $TESTER_PARTICIPANT
    python room_manager.py my-room --identity alice
    python room_manager.py my-room --no-open   # just print the URL
"""

import argparse
import asyncio
import os
import webbrowser
from datetime import timedelta
from urllib.parse import urlencode
from livekit import api


MEET_URL = os.environ.get("MEET_URL", "http://localhost:3000")


def ws_url(url: str) -> str:
    """The browser client wants ws:// where the server API wants http://."""
    return url.replace("https://", "wss://").replace("http://", "ws://")


def token(room: str, identity: str, ttl: timedelta) -> str:
    return (
        api.AccessToken()
        .with_identity(identity)
        .with_name(identity)
        .with_grants(api.VideoGrants(room_join=True, room=room))
        .with_ttl(ttl)
        .to_jwt()
    )


def create_url(token: str) -> str:
    params = urlencode(
        {"liveKitUrl": ws_url(os.environ["LIVEKIT_URL"]), "token": token}
    )
    return f"{MEET_URL}/custom?{params}"


async def create_room(name: str, empty_timeout: int) -> api.Room:
    lkapi = api.LiveKitAPI()
    try:
        # Create is idempotent: an existing room comes back unchanged.
        return await lkapi.room.create_room(
            api.CreateRoomRequest(name=name, empty_timeout=empty_timeout)
        )
    finally:
        await lkapi.aclose()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("room", nargs="?", default=os.environ.get("ROOM", "my-room"))
    parser.add_argument(
        "--identity", default=os.environ.get("TESTER_PARTICIPANT", "tester")
    )
    parser.add_argument("--valid-for", type=int, default=24, help="token TTL in hours")
    parser.add_argument(
        "--empty-timeout",
        type=int,
        default=300,
        help="seconds before an empty room closes",
    )
    parser.add_argument("--no-open", action="store_true", help="print the URL only")
    args = parser.parse_args()

    room = await create_room(args.room, args.empty_timeout)
    print(f"room: {room.name} (sid {room.sid}, {room.num_participants} participants)")

    url = create_url(token(args.room, args.identity, timedelta(hours=args.valid_for)))
    print(url)

    if not args.no_open:
        webbrowser.open(url)


if __name__ == "__main__":
    asyncio.run(main())
