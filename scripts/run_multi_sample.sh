#!/usr/bin/env bash
# Join SPEAKERS fake participants publishing the recording, repeated LOOPS times.
#
# Default: everyone talks at the same time, each saying different sentences
# (speaker i plays the recording rotated by i/SPEAKERS of its length).
#
# --turns: a conversation. The recording is split into sentences at its pauses
# and the sentences are dealt out round-robin, so one speaker talks at a time
# while the others stay silent.
#
# Usage: scripts/run_multi_sample.sh [--turns] [ROOM] [SPEAKERS=3] [LOOPS=2]
set -euo pipefail

TURNS=false
args=()
for arg in "$@"; do
  case $arg in
    --turns) TURNS=true ;;
    -h | --help) sed -n '2,11s/^# \{0,1\}//p' "$0"; exit 0 ;;
    *) args+=("$arg") ;;
  esac
done
set -- "${args[@]}"

SOURCE=temp/harvard.wav
ROOM=${1:-${ROOM:-default}}
SPEAKERS=${2:-3}
LOOPS=${3:-2}

duration=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$SOURCE")
length=$(echo "$duration * $LOOPS" | bc -l)

# Prints an ffmpeg expression that is true while it is speaker $1's turn. Turns
# are cut at the middle of each pause between sentences.
turn_expr() {
  ffmpeg -v info -i "$SOURCE" -af silencedetect=noise=-30dB:d=0.3 -f null - 2>&1 |
    awk -v me="$1" -v n="$SPEAKERS" -v loops="$LOOPS" -v dur="$duration" '
      /silence_start:/ { start = $NF }
      /silence_end:/ {
        match($0, /silence_end: [0-9.]+/)
        end = substr($0, RSTART + 13, RLENGTH - 13) + 0
        if (start > 0 && end < dur - 0.05) cuts[++c] = (start + end) / 2
      }
      END {
        cuts[0] = 0; cuts[c + 1] = dur
        for (l = 0; l < loops; l++)
          for (k = 0; k <= c; k++)
            if ((l * (c + 1) + k) % n == me) {
              expr = expr sep sprintf("between(t,%.3f,%.3f)", l * dur + cuts[k], l * dur + cuts[k + 1])
              sep = "+"
            }
        print (expr == "" ? "0" : expr)
      }'
}

trap 'kill $(jobs -p) 2>/dev/null || true' INT TERM EXIT

for ((i = 0; i < SPEAKERS; i++)); do
  if $TURNS; then
    file="temp/harvard_turns_s${SPEAKERS}_${i}_l${LOOPS}.ogg"
    filter="atrim=duration=$length,asetpts=N/SR/TB,volume=0:enable='not($(turn_expr "$i"))'"
  else
    offset=$(echo "$i * $duration / $SPEAKERS" | bc -l)
    file="temp/harvard_s${SPEAKERS}_${i}_l${LOOPS}.ogg"
    filter="atrim=start=$offset:duration=$length,asetpts=N/SR/TB"
  fi

  if [[ ! -f $file ]]; then
    # lk --publish takes Ogg Opus. The looped input wraps the recording around;
    # asetpts renumbers the timestamps across the seam, otherwise lk stalls
    # there for as long as the offset.
    ffmpeg -v error -stream_loop -1 -i "$SOURCE" -af "$filter" \
      -ac 1 -ar 48000 -c:a libopus -b:a 32k "$file"
  fi

  lk room join \
    --url "$LIVEKIT_URL" --api-key "$LIVEKIT_API_KEY" --api-secret "$LIVEKIT_API_SECRET" \
    --identity "sim-user-$((i + 1))" \
    --publish "$file" \
    --exit-after-publish \
    "$ROOM" &
done

wait
