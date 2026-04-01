#!/bin/sh
# Usage:
#   curl -s https://pipeviz.org/run | sh -s my-pipeline.json
#   PIPEVIZ_JSON=/path/to/graph.json curl -s https://pipeviz.org/run | sh
set -e

JSON="${1:-$PIPEVIZ_JSON}"
PORT="${PIPEVIZ_PORT:-8000}"

if [ -z "$JSON" ]; then
  echo "usage: curl -s https://pipeviz.org/run | sh -s <path-to-pipeviz.json>"
  echo "   or: export PIPEVIZ_JSON=/path/to/graph.json"
  exit 1
fi

if [ ! -f "$JSON" ]; then
  echo "error: $JSON not found"
  exit 1
fi

curl -s https://pipeviz.org/pipeviz.html -o /tmp/pipeviz.html
cp "$JSON" /tmp/pipeviz.json

cleanup() { kill "$SERVER_PID" 2>/dev/null; }
trap cleanup EXIT INT TERM

python3 -m http.server "$PORT" -d /tmp >/dev/null 2>&1 &
SERVER_PID=$!
sleep 0.3

URL="http://localhost:${PORT}/pipeviz.html?url=http://localhost:${PORT}/pipeviz.json"

# Cross-platform open
if command -v open >/dev/null 2>&1; then
  open "$URL"
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$URL"
else
  echo "Open: $URL"
fi

echo "pipeviz serving at $URL"
echo "press enter to stop..."
read _ </dev/tty || true
