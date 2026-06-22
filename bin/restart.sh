#!/bin/bash
# Restart tg-client-telethon
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$SCRIPT_DIR/shutdown.sh"
sleep 2
bash "$SCRIPT_DIR/startup.sh"
