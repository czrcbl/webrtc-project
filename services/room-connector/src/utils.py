import os

from livekit import api

# TODO: refactor to args
LIVEKIT_URL = os.environ["LIVEKIT_URL"]
LIVEKIT_API_KEY = os.environ["LIVEKIT_API_KEY"]
LIVEKIT_API_SECRET = os.environ["LIVEKIT_API_SECRET"]
IDENTITY = os.getenv("LISTENER_IDENTITY", "listener")


def make_token(room_name: str) -> str:
    grants = api.VideoGrants(
        room_join=True,
        room=room_name,
        can_subscribe=True,
        can_publish=False,  # no audio/video from us
        can_publish_data=True,  # required for send_text
        hidden=True,  # don't appear in the participant list
    )
    return (
        api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
        .with_identity(IDENTITY)
        .with_name("Listener")
        .with_grants(grants)
        .to_jwt()
    )
