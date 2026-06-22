#!/bin/bash
# Start tg-client-telethon
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="$BASE_DIR/bin/app.pid"
LOG_FILE="$BASE_DIR/logs/app.log"
export PATH="$HOME/.local/bin:$PATH"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Already running (PID: $PID)"
        exit 1
    fi
    rm -f "$PID_FILE"
fi

# Ensure directories exist
mkdir -p "$BASE_DIR/logs"
mkdir -p "$BASE_DIR/account/waitLogin"
mkdir -p "$BASE_DIR/account/loginSuccess"
mkdir -p "$BASE_DIR/account/loginFailed"

# Start the application
cd "$BASE_DIR"
nohup python3 main.py > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "Started (PID: $!)"
