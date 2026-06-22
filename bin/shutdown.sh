#!/bin/bash
# Stop tg-client-telethon
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="$BASE_DIR/bin/app.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "Not running (no PID file)"
    # Try to find and kill by process name
    PID=$(pgrep -f "python3 main.py" | head -1)
    if [ -n "$PID" ]; then
        kill "$PID" 2>/dev/null
        echo "Killed process $PID"
    fi
    exit 0
fi

PID=$(cat "$PID_FILE")
if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    sleep 2
    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID"
    fi
    echo "Stopped (PID: $PID)"
else
    echo "Process not running (stale PID file)"
fi
rm -f "$PID_FILE"
