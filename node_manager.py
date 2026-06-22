"""Node manager - handles node identity, registration, and heartbeat."""
import hashlib
import logging
import os
import socket
import asyncio
import uuid

import httpx

import config
import database

logger = logging.getLogger(__name__)

# Current node info (populated on startup)
NODE_ID: str = ""
PUBLIC_IP: str = ""
PRIVATE_IP: str = ""
NODE_DIR: str = ""

HEARTBEAT_INTERVAL = 15  # seconds


def _get_private_ip() -> str:
    """Get private/internal IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def _get_public_ip() -> str:
    """Get public IP address."""
    services = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]
    for url in services:
        try:
            resp = httpx.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.text.strip()
        except Exception:
            continue
    return ""


def _generate_node_id(node_dir: str, public_ip: str, private_ip: str) -> str:
    """Generate unique node ID: MD5 middle 16 chars of fingerprint string.

    Fingerprint = {hostname}|{node_dir}|{public_ip}|{private_ip}|{uuid}
    The uuid part ensures uniqueness even on the same machine with same dir.
    """
    hostname = socket.gethostname()
    # Include a random uuid to ensure uniqueness for multiple nodes on same machine
    raw = f"{hostname}|{node_dir}|{public_ip}|{private_ip}|{uuid.uuid4().hex}"
    md5_hash = hashlib.md5(raw.encode("utf-8")).hexdigest()
    # Middle 16 chars: chars 8..24
    return md5_hash[8:24]


def _get_node_id_file_path() -> str:
    """Get the path to the node ID file in data directory."""
    data_dir = os.path.join(config.BASE_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "node_id")


def _load_node_id() -> str | None:
    """Load node ID from file, return None if not exists."""
    path = _get_node_id_file_path()
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                nid = f.read().strip()
                if nid:
                    return nid
        except Exception as e:
            logger.warning(f"Failed to read node ID file: {e}")
    return None


def _save_node_id(node_id: str):
    """Save node ID to file."""
    path = _get_node_id_file_path()
    with open(path, "w") as f:
        f.write(node_id)
    logger.info(f"Node ID saved to {path}: {node_id}")


async def init_node():
    """Initialize node: load or generate node ID, register in database."""
    global NODE_ID, PUBLIC_IP, PRIVATE_IP, NODE_DIR

    NODE_DIR = config.BASE_DIR
    PUBLIC_IP = _get_public_ip()
    PRIVATE_IP = _get_private_ip()
    logger.info(f"Node IPs: public={PUBLIC_IP}, private={PRIVATE_IP}")

    # Try to load existing node ID
    existing_id = _load_node_id()
    if existing_id:
        NODE_ID = existing_id
        logger.info(f"Loaded existing node ID: {NODE_ID}")
    else:
        # Generate new node ID
        NODE_ID = _generate_node_id(NODE_DIR, PUBLIC_IP, PRIVATE_IP)
        _save_node_id(NODE_ID)
        logger.info(f"Generated new node ID: {NODE_ID}")

    # Register/update node in database
    await _register_node()
    logger.info(f"Node registered: id={NODE_ID}, dir={NODE_DIR}")


async def _register_node():
    """Register or update this node in the tg_cluster_node table."""
    async with database.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT INTO tg_cluster_node
                   (node_id, public_ip, private_ip, node_dir, node_port, last_active_time,
                    total_account_count, online_account_count)
                   VALUES (%s, %s, %s, %s, %s, NOW(), 0, 0)
                   ON DUPLICATE KEY UPDATE
                       public_ip = VALUES(public_ip),
                       private_ip = VALUES(private_ip),
                       node_dir = VALUES(node_dir),
                       node_port = VALUES(node_port),
                       last_active_time = NOW()""",
                (NODE_ID, PUBLIC_IP, PRIVATE_IP, NODE_DIR, config.PORT),
            )
            await conn.commit()


async def heartbeat_loop():
    """Background task: update node heartbeat every HEARTBEAT_INTERVAL seconds."""
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await _update_heartbeat()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")


async def _update_heartbeat():
    """Update last_active_time and account counts in tg_cluster_node."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Count total accounts assigned to this node
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM tg_telethon_account WHERE node_id = %s AND is_deleted = 0",
                    (NODE_ID,),
                )
                total = (await cur.fetchone())["cnt"]

                # Count online accounts
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM tg_telethon_account WHERE node_id = %s AND status = 'online'",
                    (NODE_ID,),
                )
                online = (await cur.fetchone())["cnt"]

                await cur.execute(
                    """UPDATE tg_cluster_node SET
                           last_active_time = NOW(),
                           total_account_count = %s,
                           online_account_count = %s,
                           public_ip = %s,
                           private_ip = %s
                       WHERE node_id = %s""",
                    (total, online, PUBLIC_IP, PRIVATE_IP, NODE_ID),
                )
                await conn.commit()
    except Exception as e:
        logger.error(f"Heartbeat update failed: {e}")


def get_node_info_str() -> str:
    """Get a brief node info string for notifications."""
    return f"[节点:{NODE_ID[:8]}.. IP:{PUBLIC_IP}]"
