#!/usr/bin/env bash
RECORDING_PATH=temp/harvard_converted.ogg

lk room join \
  --url "$LIVEKIT_URL" --api-key "$LIVEKIT_API_KEY" --api-secret "$LIVEKIT_API_SECRET" \
  --identity sim-user-1 \
  --publish $RECORDING_PATH \
  $1