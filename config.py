"""Application configuration for tg-gc-server (cluster node)."""
import os
from datetime import datetime, timezone, timedelta

# Beijing timezone (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))


def to_beijing(dt):
    """Convert a datetime to Beijing time."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
    return dt


# Server
HOST = os.getenv("APP_HOST", "0.0.0.0")
PORT = int(os.getenv("APP_PORT", "8807"))

# Database (MySQL) - must point to the main server's MySQL
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "TgGc@2026!Secure")
DB_NAME = os.getenv("DB_NAME", "tg_gc")

# Telegram Bot Notification
BOT_TOKEN = os.getenv("BOT_TOKEN", "8534398194:AAF6CKDeS_yGeo167C4znOq9cR3porDGJa0")
BOT_CHAT_ID = os.getenv("BOT_CHAT_ID", "-5181774632")

# JWT Secret for web client authentication
JWT_SECRET = os.getenv("JWT_SECRET", "tg-gc-secret-key-2026")
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "24"))

# Auto-reply
REPLY_API_URL = os.getenv("REPLY_API_URL", "http://127.0.0.1:8000/generate-reply")
AUTO_REPLY_INTERVAL = int(os.getenv("AUTO_REPLY_INTERVAL", "300"))  # seconds (default 5 min)

# Intervals (seconds)
HEARTBEAT_INTERVAL = 15
LOGIN_POLL_INTERVAL = 15
CONTACT_ADDER_INTERVAL = 15
CONCURRENT_LOGIN_LIMIT = 15  # max concurrent logins at a time

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
ACCOUNT_DIR = os.path.join(DATA_DIR, "account")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ACCOUNT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
