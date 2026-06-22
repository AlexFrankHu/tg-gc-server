"""Application configuration."""
import os
from datetime import datetime, timezone, timedelta

# Beijing timezone (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))


def to_beijing(dt):
    """Convert a datetime to Beijing time.
    - If dt is timezone-aware (e.g. Telethon UTC), convert to Beijing time and strip tzinfo.
    - If dt is naive, assume it's already Beijing time and return as-is.
    - Returns None if dt is None.
    """
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
    return dt

# Server
HOST = os.getenv("APP_HOST", "0.0.0.0")
PORT = int(os.getenv("APP_PORT", "8807"))

# Database (MySQL)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "XkOVjlR6FvmONtLi75BS")
DB_NAME = os.getenv("DB_NAME", "tg-client-server")

# Telegram Bot Notification
BOT_TOKEN = os.getenv("BOT_TOKEN", "8534398194:AAF6CKDeS_yGeo167C4znOq9cR3porDGJa0")
BOT_CHAT_ID = os.getenv("BOT_CHAT_ID", "-5181774632")

# JWT Secret for web client authentication
JWT_SECRET = os.getenv("JWT_SECRET", "tg-telethon-secret-key-2024")
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "24"))

# Auto-reply
REPLY_API_URL = os.getenv("REPLY_API_URL", "http://127.0.0.1:8000/generate-reply")
AUTO_REPLY_INTERVAL = int(os.getenv("AUTO_REPLY_INTERVAL", "300"))  # seconds (default 5 min)

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNT_DIR = os.path.join(BASE_DIR, "account")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
