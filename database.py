"""Database layer for tg-gc-server (cluster node).

All tables use utf8mb4. Node-scoped queries filter by node_id.
"""
import asyncio
import functools
import logging
from datetime import datetime

import aiomysql
from pymysql.err import OperationalError

import config

logger = logging.getLogger(__name__)

pool: aiomysql.Pool = None

# MySQL 锁相关错误: 1205=Lock wait timeout, 1213=Deadlock。
# 这类错误是可重试的(与统计任务等大事务抢锁导致), 重试通常即可成功。
_LOCK_ERROR_CODES = (1205, 1213)


def retry_on_lock(max_retries: int = 3, base_delay: float = 0.2):
    """Decorator: retry an async DB write on MySQL lock-wait-timeout(1205)/deadlock(1213).

    Each decorated function acquires its own autocommit connection and its writes are
    idempotent (upsert / single-row update), so re-running the whole function is safe.
    """
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await fn(*args, **kwargs)
                except OperationalError as e:
                    code = e.args[0] if e.args else None
                    if code in _LOCK_ERROR_CODES and attempt < max_retries:
                        attempt += 1
                        delay = base_delay * attempt
                        logger.warning(
                            f"DB lock error ({code}) on {fn.__name__}, "
                            f"retry {attempt}/{max_retries} after {delay:.2f}s"
                        )
                        await asyncio.sleep(delay)
                        continue
                    raise
        return wrapper
    return decorator


async def init_db():
    """Initialize the database connection pool."""
    global pool
    pool = await aiomysql.create_pool(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        db=config.DB_NAME,
        charset="utf8mb4",
        autocommit=True,
        minsize=2,
        maxsize=5,
        cursorclass=aiomysql.DictCursor,
    )
    logger.info(f"Database pool initialized: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")


async def close_db():
    """Close the database pool."""
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()
        pool = None


# =============================================================================
# Account queries
# =============================================================================

async def get_all_accounts() -> list[dict]:
    """Get all non-deleted accounts."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_telethon_account WHERE is_deleted = 0 ORDER BY id"
            )
            return await cur.fetchall()


async def get_accounts_by_node(node_id: str) -> list[dict]:
    """Get all accounts assigned to a specific node."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_telethon_account WHERE node_id = %s AND is_deleted = 0 ORDER BY id",
                (node_id,),
            )
            return await cur.fetchall()


async def get_accounts_by_node_and_status(node_id: str, status: str) -> list[dict]:
    """Get accounts by node and status (e.g. 'login1', 'login2', 'online')."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_telethon_account WHERE node_id = %s AND status = %s AND is_deleted = 0",
                (node_id, status),
            )
            return await cur.fetchall()


async def get_restricted_accounts_by_node(node_id: str) -> list[dict]:
    """Get accounts that are restricted or frozen for a node, regardless of status."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_telethon_account WHERE node_id = %s AND (is_restricted = 1 OR is_frozen = 1) "
                "AND status != 'online' AND is_deleted = 0",
                (node_id,),
            )
            return await cur.fetchall()


async def get_account_by_phone(phone: str) -> dict | None:
    """Get a single account by phone number."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_telethon_account WHERE phone = %s AND is_deleted = 0",
                (phone,),
            )
            return await cur.fetchone()


async def get_account_by_id(account_id: int) -> dict | None:
    """Get a single account by ID."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_telethon_account WHERE id = %s AND is_deleted = 0",
                (account_id,),
            )
            return await cur.fetchone()


@retry_on_lock()
async def update_account_status(phone: str, status: str):
    """Update account status in tg_telethon_account and tg_import_account."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE tg_telethon_account SET status = %s, update_time = NOW() WHERE phone = %s",
                (status, phone),
            )
            await cur.execute(
                "UPDATE tg_import_account SET status = %s, update_time = NOW() WHERE phone = %s",
                (status, phone),
            )


async def update_account_on_login(phone: str, tg_user_id: int | None,
                                   nickname: str, username: str | None,
                                   country: str | None, device_kwargs: dict,
                                   node_id: str):
    """Update account info after successful login."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_telethon_account SET
                       status = 'online',
                       tg_user_id = %s,
                       nickname = %s,
                       username = %s,
                       country = %s,
                       device_model = %s,
                       system_version = %s,
                       app_version = %s,
                       lang_code = %s,
                       system_lang_code = %s,
                       node_id = %s,
                       last_login_time = NOW(),
                       update_time = NOW()
                   WHERE phone = %s""",
                (
                    tg_user_id, nickname, username, country,
                    device_kwargs.get("device_model"),
                    device_kwargs.get("system_version"),
                    device_kwargs.get("app_version"),
                    device_kwargs.get("lang_code"),
                    device_kwargs.get("system_lang_code"),
                    node_id, phone,
                ),
            )


async def save_device_fingerprint(phone: str, fp: dict):
    """Save device fingerprint to account."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_telethon_account SET
                       device_model = %s, system_version = %s,
                       app_version = %s, lang_code = %s, system_lang_code = %s
                   WHERE phone = %s""",
                (
                    fp.get("device_model"), fp.get("system_version"),
                    fp.get("app_version"), fp.get("lang_code"),
                    fp.get("system_lang_code"), phone,
                ),
            )


# =============================================================================
# Login log
# =============================================================================

async def insert_login_log(phone: str, result: str, reason: str = None,
                           tg_user_id: int = None, nickname: str = None,
                           proxy_info: str = None, node_id: str = None):
    """Insert a login log entry."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT INTO tg_login_log
                   (phone, result, reason, tg_user_id, nickname, proxy_info, node_id, login_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())""",
                (phone, result, reason, tg_user_id, nickname, proxy_info, node_id),
            )


# =============================================================================
# Contacts
# =============================================================================

async def get_contact(tg_account_id: int, user_id: int) -> dict | None:
    """Get a specific contact."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_contact WHERE tg_account_id = %s AND user_id = %s",
                (tg_account_id, user_id),
            )
            return await cur.fetchone()


@retry_on_lock()
async def upsert_contact(tg_account_id: int, user_id: int, first_name: str = None,
                          last_name: str = None, nickname: str = None,
                          username: str = None, phone_number: str = None,
                          is_mutual: bool = False, is_bot: bool = False,
                          is_premium: bool = False, user_type: str = "regular",
                          source: str = "natural", node_id: str = None,
                          access_hash: int = None, contact_type: str = "real"):
    """Insert or update a contact.

    access_hash is required to message a fake contact (contact_type='fake'), which is
    not in the account's TG contact list and therefore not guaranteed to be cached in
    the local Telethon session.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT INTO tg_contact
                   (tg_account_id, user_id, access_hash, first_name, last_name, nickname,
                    username, phone_number, is_mutual, is_bot, is_premium,
                    user_type, source, contact_type, node_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       access_hash = COALESCE(VALUES(access_hash), access_hash),
                       first_name = VALUES(first_name),
                       last_name = VALUES(last_name),
                       nickname = VALUES(nickname),
                       username = VALUES(username),
                       phone_number = VALUES(phone_number),
                       is_mutual = VALUES(is_mutual),
                       is_bot = VALUES(is_bot),
                       is_premium = VALUES(is_premium),
                       user_type = VALUES(user_type),
                       node_id = VALUES(node_id)""",
                (
                    tg_account_id, user_id, access_hash, first_name, last_name, nickname,
                    username, phone_number, is_mutual, is_bot, is_premium,
                    user_type, source, contact_type, node_id,
                ),
            )


@retry_on_lock()
async def update_contact_timestamps(tg_account_id: int, user_id: int,
                                     last_send_time: datetime = None,
                                     last_receive_time: datetime = None):
    """Update contact message timestamps."""
    sets = []
    params = []
    if last_send_time:
        sets.append("last_send_time = %s")
        params.append(last_send_time)
    if last_receive_time:
        sets.append("last_receive_time = %s")
        params.append(last_receive_time)
    if not sets:
        return
    params.extend([tg_account_id, user_id])
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE tg_contact SET {', '.join(sets)} WHERE tg_account_id = %s AND user_id = %s",
                params,
            )


@retry_on_lock()
async def update_contact_msg_counts(tg_account_id: int, user_id: int,
                                     total: int, account_sent: int, friend_sent: int):
    """Update contact message counts."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_contact SET
                       total_msg_count = %s, account_sent_count = %s, friend_sent_count = %s
                   WHERE tg_account_id = %s AND user_id = %s""",
                (total, account_sent, friend_sent, tg_account_id, user_id),
            )


# =============================================================================
# Chat messages
# =============================================================================

@retry_on_lock()
async def insert_chat_message(tg_account_id: int, chat_id: int, message_id: int,
                               sender_user_id: int = None, is_outgoing: bool = False,
                               send_time: datetime = None, content_type: str = None,
                               text_content: str = None, media_file_id: int = None,
                               media_file_size: int = None, media_mime_type: str = None,
                               node_id: str = None):
    """Insert a chat message (ignore duplicates).

    群消息(聊天ID为负数, 即群组/超级群/频道)不保存, 只保存私聊消息。
    """
    if chat_id is not None and chat_id < 0:
        return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT IGNORE INTO tg_chat_message
                   (tg_account_id, chat_id, message_id, sender_user_id, is_outgoing,
                    send_time, content_type, text_content, media_file_id,
                    media_file_size, media_mime_type, node_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    tg_account_id, chat_id, message_id, sender_user_id, is_outgoing,
                    send_time, content_type, text_content, media_file_id,
                    media_file_size, media_mime_type, node_id,
                ),
            )


async def get_message_by_file_id(tg_account_id: int, file_id: str) -> dict | None:
    """Get a chat message by file ID."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_chat_message WHERE tg_account_id = %s AND media_file_id = %s LIMIT 1",
                (tg_account_id, file_id),
            )
            return await cur.fetchone()


async def get_latest_message_id(tg_account_id: int, chat_id: int) -> int | None:
    """Get the latest message ID for a chat."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT MAX(message_id) as max_id FROM tg_chat_message WHERE tg_account_id = %s AND chat_id = %s",
                (tg_account_id, chat_id),
            )
            row = await cur.fetchone()
            return row["max_id"] if row else None


async def get_recent_messages(tg_account_id: int, chat_id: int, limit: int = 20) -> list[dict]:
    """Get recent messages for a chat."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT * FROM tg_chat_message
                   WHERE tg_account_id = %s AND chat_id = %s
                   ORDER BY message_id DESC LIMIT %s""",
                (tg_account_id, chat_id, limit),
            )
            rows = await cur.fetchall()
            return list(reversed(rows))


# =============================================================================
# Auto-reply log
# =============================================================================

async def insert_auto_reply_log(account_phone: str, account_nickname: str,
                                 friend_user_id: int, friend_nickname: str,
                                 friend_phone: str, trigger_type: str,
                                 state: int, request_params: str,
                                 chat_context: str, reply_content: str,
                                 send_result: str, error_reason: str = None,
                                 node_id: str = None):
    """Insert an auto-reply log entry."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT INTO tg_auto_reply_log
                   (account_phone, account_nickname, friend_user_id, friend_nickname,
                    friend_phone, trigger_type, state, request_params, chat_context,
                    reply_content, send_result, error_reason, node_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    account_phone, account_nickname, friend_user_id, friend_nickname,
                    friend_phone, trigger_type, state, request_params, chat_context,
                    reply_content, send_result, error_reason, node_id,
                ),
            )


# =============================================================================
# Send fail log
# =============================================================================

async def insert_send_fail_log(phone: str, tg_account_id: int, user_id: int,
                                content_type: str, content: str,
                                error_reason: str, send_time: datetime = None,
                                node_id: str = None):
    """Insert a send failure log."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT INTO tg_send_fail_log
                   (phone, tg_account_id, user_id, content_type, content,
                    error_reason, send_time, node_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (phone, tg_account_id, user_id, content_type, content,
                 error_reason, send_time or datetime.now(), node_id),
            )


# =============================================================================
# Contact adder (assign log)
# =============================================================================

async def get_pending_contacts_by_node(node_id: str) -> list[dict]:
    """Get pending contacts to add for this node from tg_contact_assign_log."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT ca.*, ta.phone as account_phone
                   FROM tg_contact_assign_log ca
                   JOIN tg_telethon_account ta ON ca.account_id = ta.id
                   WHERE ca.node_id = %s AND ca.status = 'pending'
                   ORDER BY ca.account_id, ca.create_time""",
                (node_id,),
            )
            return await cur.fetchall()


@retry_on_lock()
async def update_contact_assign_status(assign_id: int, status: str,
                                        result_user_id: int = None,
                                        error_reason: str = None):
    """Update contact assignment status."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_contact_assign_log SET
                       status = %s, result_user_id = %s, error_reason = %s,
                       update_time = NOW()
                   WHERE id = %s""",
                (status, result_user_id, error_reason, assign_id),
            )


async def increment_retry_count(assign_id: int) -> int:
    """Increment retry_count for a contact assign record and return new value."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_contact_assign_log SET
                       retry_count = IFNULL(retry_count, 0) + 1,
                       update_time = NOW()
                   WHERE id = %s""",
                (assign_id,),
            )
            await cur.execute(
                "SELECT retry_count FROM tg_contact_assign_log WHERE id = %s",
                (assign_id,),
            )
            row = await cur.fetchone()
            return row["retry_count"] if row else 0


async def mark_account_restricted(account_id: int, phone: str):
    """Mark an account as restricted (is_restricted=1), without changing status."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_telethon_account SET
                       is_restricted = 1, update_time = NOW()
                   WHERE id = %s""",
                (account_id,),
            )


async def mark_account_frozen(account_id: int, phone: str):
    """Mark an account as frozen by Telegram. A frozen account is always restricted too."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_telethon_account SET
                       is_frozen = 1, is_restricted = 1, update_time = NOW()
                   WHERE id = %s AND (is_frozen = 0 OR is_frozen IS NULL)""",
                (account_id,),
            )
            if cur.rowcount:
                logger.warning("[%s] 账号被TG冻结(frozen), 已标记 is_frozen=1 + is_restricted=1", phone)


def is_account_blocked(account: dict) -> bool:
    """An account must not perform any Telegram action when restricted or frozen."""
    return bool(account.get("is_restricted") or account.get("is_frozen"))


async def fail_pending_contacts_for_account(account_id: int, node_id: str, error_reason: str):
    """Mark all pending contact assignments for an account as failed."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_contact_assign_log SET
                       status = 'failed', error_reason = %s, update_time = NOW()
                   WHERE (account_id = %s OR tg_account_id = %s)
                     AND node_id = %s AND status = 'pending'""",
                (error_reason, account_id, account_id, node_id),
            )


# =============================================================================
# Greetings / Opening
# =============================================================================

async def get_greetings() -> list[dict]:
    """Get all enabled greeting messages."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_greeting WHERE is_enabled = 1 ORDER BY sort_order, id"
            )
            return await cur.fetchall()


async def get_openings() -> list[dict]:
    """Get all enabled opening messages."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM tg_opening WHERE is_enabled = 1 ORDER BY sort_order, id"
            )
            return await cur.fetchall()


# =============================================================================
# Account message stats
# =============================================================================

async def update_account_msg_counts(phone: str, total: int, sent: int, recv: int):
    """Update account-level message counts."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE tg_telethon_account SET
                       total_msg_count = %s, sent_msg_count = %s, recv_msg_count = %s
                   WHERE phone = %s""",
                (total, sent, recv, phone),
            )


# =============================================================================
# Node-related queries (for cluster)
# =============================================================================

async def get_active_nodes(minutes: int = 2) -> list[dict]:
    """Get active nodes (last_active_time within N minutes)."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT * FROM tg_cluster_node
                   WHERE last_active_time >= DATE_SUB(NOW(), INTERVAL %s MINUTE)
                   ORDER BY online_account_count ASC""",
                (minutes,),
            )
            return await cur.fetchall()
