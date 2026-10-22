from livekit import api
from src.config import settings


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
        api.AccessToken(settings.livekit_api_key, settings.livekit_api_secret.get_secret_value())
        .with_identity(settings.listener_identity)
        .with_name("Listener")
        .with_kind("agent")  # Needed to keep sender_identity
        .with_grants(grants)
        .to_jwt()
    )
