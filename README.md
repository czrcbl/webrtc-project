# WebRTC Project

Sample project for providing WebRTC powered application with the ability to provide realtime subtitles for the 
audio streams of each participant and use a Language model to provida summaries of the ongoing conversation.

The project has the following components:
- A stock Livekit server;
- The Meet web client, modified to show the lk.transcription text stream topic on screen;
- `transcriber`, the application per se. It uses LiveKit's Python SDK to connect to the room, subscribe into the audio tracks. 
It sends the audio stream to a STT server, injects the results back into the room as transcriptions and uses a LLM to provide summaries of the call, that are published into the chat topic.

## Usage

The transcriber needs an [Anthropic](https://console.anthropic.com/) and a
[Deepgram](https://console.deepgram.com/) API key.
You can set them in a `mise.local.toml` file.

``` toml
[env]
DEEPGRAM_API_KEY = ""
ANTHROPIC_API_KEY = ""
```

Or source them in your shell:
```bash
export DEEPGRAM_API_KEY=""
export ANTHROPIC_API_KEY=""
```

The projects use `mise` to manager project dependencies, environment and tasks.
`uv` is use for managing python environments and dependencies.

For running with all dependencies natively, jump to the [Running](#running) section.

The docker-compose file will spin LiveKit using the official image and build Meet from this project's forked repository.
```bash
docker compose up
# podman compose up
```
Install python deps with uv:
```bash
uv sync --package transcriber
``` 

Running the transcriber server:
```bash
mise run transcriber
# mise run transcriber-en  # Transcriber server preset for english
```

The `stt-service` was deprecated and requires Nvidia related dependencies, to try it out:
```bash
# Install dependencies for stt-service
uv sync --package stt-service

# Run the stt-service server
mise run stt-service
```

### Getting the environment ready

Install dev dependencies:
```bash
mise install
```

Run the bootstrap tasks on mise file. It downloads all repos, install dependencies and build the packages.
```bash
mise bootstrap
```

Create the python virtual environment for the project and install the dependencies:
```bash
uv sync --all-packages
```

### Running

Set up the API keys as instructed in the Usage section.

Spinning the dev servers from the local repos and from code for transcri
```bash
mise run server-dev  # Levekit
mise run meet-dev    # Meet server
mise run transcriber  # Transcriber server
# mise run transcriber-en  # Transcriber server preset for english
```

Helper script for spawning multiple agents in a room. 
```bashc
 sh scripts/run_multi_sample.sh <room-code> num_speakers loops (--turns)
 ```




