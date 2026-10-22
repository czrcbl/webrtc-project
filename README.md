# WebRTC Project

## Usage

Spinning the dev servers:
```bash
mise run server-dev  # Levekit
mise run meet-dev    # Meet server
mise run transcriber  # Transcriber server

# mise run transcriber-en  # Transcriber server preset for english
```

Spawning test participants:
```bashc
 sh scripts/run_multi_sample.sh <room-code> num_speakers loops (--turns)
 ```

## Development
```bash
uv sync --all-packages
```

