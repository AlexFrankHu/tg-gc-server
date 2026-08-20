"""Application configuration for tg-gc-server (cluster node)."""
import os
import socket
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


def _is_port_available(port: int) -> bool:
    """Check if a port is available."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", port))
        s.close()
        return True
    except OSError:
        return False


def _find_available_port(start_port: int = 9990, max_tries: int = 10) -> int:
    """Find an available port starting from start_port."""
    for offset in range(max_tries):
        port = start_port + offset
        if _is_port_available(port):
            return port
    return start_port


def _get_port_file_path() -> str:
    """Get the path to the persisted port file."""
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "node_port")


def _load_or_assign_port(base_port: int) -> int:
    """Load persisted port if available, otherwise find a new one and save it."""
    port_file = _get_port_file_path()
    # Try to load saved port
    if os.path.exists(port_file):
        try:
            with open(port_file, "r") as f:
                saved_port = int(f.read().strip())
            if _is_port_available(saved_port):
                return saved_port
        except (ValueError, IOError):
            pass
    # Find a new available port and save it
    port = _find_available_port(base_port)
    try:
        with open(port_file, "w") as f:
            f.write(str(port))
    except IOError:
        pass
    return port


# Server
HOST = os.getenv("APP_HOST", "0.0.0.0")
_BASE_PORT = int(os.getenv("APP_PORT", "9990"))
PORT = _load_or_assign_port(_BASE_PORT)

# Database (MySQL) - must point to the main server's MySQL
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "TgGc@2026!Secure")
DB_NAME = os.getenv("DB_NAME", "tg_gc")

# Telegram Bot Notification
BOT_TOKEN = os.getenv("BOT_TOKEN", "8995484464:AAHWjku4XUj0Y-yJs5AYmyRLLCEH-wUSh0A")
BOT_CHAT_ID = os.getenv("BOT_CHAT_ID", "-5181774632")

# JWT Secret for web client authentication
JWT_SECRET = os.getenv("JWT_SECRET", "tg-gc-secret-key-2026")
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "24"))

# Auto-reply
REPLY_API_URL = os.getenv("REPLY_API_URL", "http://172.22.16.41:8001/generate-reply")
AUTO_REPLY_INTERVAL = int(os.getenv("AUTO_REPLY_INTERVAL", "33"))  # seconds

# Intervals (seconds)
HEARTBEAT_INTERVAL = 15
LOGIN_POLL_INTERVAL = 15
CONTACT_ADDER_INTERVAL = 15
CONCURRENT_LOGIN_LIMIT = 15  # max concurrent logins at a time
LOGIN_TIMEOUT = 60  # seconds, per-account login timeout

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
ACCOUNT_DIR = os.path.join(DATA_DIR, "account")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ACCOUNT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
