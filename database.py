"""Database operations for account management."""
import aiomysql
import config
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

pool = None

TABLE_NAME = "tg_telethon_account"
LOGIN_LOG_TABLE = "tg_login_log"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `phone` VARCHAR(32) NOT NULL COMMENT '手机号',
    `api_id` INT DEFAULT NULL COMMENT 'Telegram API ID',
    `api_hash` VARCHAR(64) DEFAULT NULL COMMENT 'Telegram API Hash',
    `tg_user_id` BIGINT DEFAULT NULL COMMENT 'Telegram用户ID',
    `nickname` VARCHAR(128) DEFAULT NULL COMMENT '昵称(firstName + lastName)',
    `username` VARCHAR(64) DEFAULT NULL COMMENT '用户名',
    `status` VARCHAR(20) NOT NULL DEFAULT 'offline' COMMENT '状态: online-在线, offline-下线, banned-已被注销',
    `last_login_time` DATETIME DEFAULT NULL COMMENT '最后登录时间',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Telethon账号管理表';
"""

CREATE_LOGIN_LOG_SQL = f"""
CREATE TABLE IF NOT EXISTS `{LOGIN_LOG_TABLE}` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `phone` VARCHAR(32) NOT NULL COMMENT '手机号',
    `result` VARCHAR(20) NOT NULL COMMENT '登录结果: success-成功, failed-失败, banned-已被注销',
    `reason` VARCHAR(512) DEFAULT NULL COMMENT '失败原因',
    `tg_user_id` BIGINT DEFAULT NULL COMMENT 'Telegram用户ID',
    `nickname` VARCHAR(128) DEFAULT NULL COMMENT '昵称',
    `proxy_info` VARCHAR(500) DEFAULT NULL COMMENT '代理信息',
    `login_time` DATETIME NOT NULL COMMENT '登录时间',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX `idx_phone` (`phone`),
    INDEX `idx_login_time` (`login_time`),
    INDEX `idx_result` (`result`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录日志表';
"""


CREATE_CONTACT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `tg_contact` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `tg_account_id` INT NOT NULL COMMENT '所属账号ID',
    `user_id` BIGINT NOT NULL COMMENT 'TG用户ID',
    `first_name` VARCHAR(128) DEFAULT NULL,
    `last_name` VARCHAR(128) DEFAULT NULL,
    `nickname` VARCHAR(256) DEFAULT NULL COMMENT '昵称',
    `username` VARCHAR(64) DEFAULT NULL COMMENT '用户名',
    `phone_number` VARCHAR(32) DEFAULT NULL COMMENT '手机号',
    `is_mutual` TINYINT(1) DEFAULT 0 COMMENT '是否互为好友',
    `is_bot` TINYINT(1) DEFAULT 0 COMMENT '是否机器人',
    `is_premium` TINYINT(1) DEFAULT 0 COMMENT '是否Premium',
    `is_verified` TINYINT(1) DEFAULT 0 COMMENT '是否认证',
    `user_type` VARCHAR(20) DEFAULT 'regular' COMMENT '类型: regular/bot/deleted',
    `restriction_reason` VARCHAR(512) DEFAULT NULL,
    `bio` TEXT DEFAULT NULL,
    `photo_small_file_id` VARCHAR(128) DEFAULT NULL,
    `photo_big_file_id` VARCHAR(128) DEFAULT NULL,
    `last_online_time` DATETIME DEFAULT NULL COMMENT '最后在线时间',
    `last_send_time` DATETIME DEFAULT NULL COMMENT '最后发送时间',
    `last_receive_time` DATETIME DEFAULT NULL COMMENT '最后接收时间',
    `auto_reply` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否开启自动回复 1开启 0关闭',
    `source` VARCHAR(20) NOT NULL DEFAULT 'natural' COMMENT '来源: import-后台导入, natural-自然添加',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_account_user` (`tg_account_id`, `user_id`),
    INDEX `idx_tg_account_id` (`tg_account_id`),
    INDEX `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友/联系人表';
"""

CREATE_IMPORT_BATCH_SQL = """
CREATE TABLE IF NOT EXISTS `tg_import_batch` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `batch_no` VARCHAR(64) NOT NULL COMMENT '批次号',
    `file_name` VARCHAR(256) DEFAULT NULL COMMENT '导入文件名',
    `total_count` INT DEFAULT 0 COMMENT '导入账号总数',
    `success_count` INT DEFAULT 0 COMMENT '登录成功数',
    `failed_count` INT DEFAULT 0 COMMENT '登录失败数',
    `waiting_count` INT DEFAULT 0 COMMENT '等待登录数',
    `import_time` DATETIME DEFAULT NULL COMMENT '导入时间',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_batch_no` (`batch_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账号导入批次表';
"""

CREATE_IMPORT_ACCOUNT_SQL = """
CREATE TABLE IF NOT EXISTS `tg_import_account` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `batch_no` VARCHAR(64) NOT NULL COMMENT '批次号',
    `phone` VARCHAR(32) NOT NULL COMMENT '手机号',
    `status` VARCHAR(20) NOT NULL DEFAULT 'waiting' COMMENT '状态: waiting-等待登录, online-已登录, failed-登录失败, banned-已注销',
    `reason` VARCHAR(512) DEFAULT NULL COMMENT '失败原因',
    `tg_user_id` BIGINT DEFAULT NULL COMMENT 'TG用户ID',
    `nickname` VARCHAR(128) DEFAULT NULL COMMENT '昵称',
    `username` VARCHAR(64) DEFAULT NULL COMMENT '用户名',
    `login_time` DATETIME DEFAULT NULL COMMENT '登录时间',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX `idx_batch_no` (`batch_no`),
    INDEX `idx_phone` (`phone`),
    INDEX `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账号导入明细表';
"""

CREATE_MESSAGE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `tg_chat_message` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `tg_account_id` INT NOT NULL COMMENT '所属账号ID',
    `chat_id` BIGINT NOT NULL COMMENT '聊天ID',
    `message_id` BIGINT NOT NULL COMMENT '消息ID',
    `sender_user_id` BIGINT DEFAULT NULL COMMENT '发送者用户ID',
    `sender_chat_id` BIGINT DEFAULT NULL COMMENT '发送者频道ID',
    `sender_name` VARCHAR(128) DEFAULT NULL COMMENT '发送者名称',
    `is_outgoing` TINYINT(1) DEFAULT 0 COMMENT '是否发出',
    `send_time` DATETIME DEFAULT NULL COMMENT '发送时间',
    `content_type` VARCHAR(20) DEFAULT 'text' COMMENT '消息类型',
    `text_content` TEXT DEFAULT NULL COMMENT '文本内容',
    `media_file_id` BIGINT DEFAULT NULL COMMENT '媒体文件ID',
    `media_file_size` BIGINT DEFAULT NULL,
    `media_mime_type` VARCHAR(64) DEFAULT NULL,
    `media_file_name` VARCHAR(256) DEFAULT NULL,
    `media_duration` INT DEFAULT NULL,
    `media_width` INT DEFAULT NULL,
    `media_height` INT DEFAULT NULL,
    `thumbnail_file_id` VARCHAR(128) DEFAULT NULL,
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_account_chat_msg` (`tg_account_id`, `chat_id`, `message_id`),
    INDEX `idx_tg_account_id` (`tg_account_id`),
    INDEX `idx_chat_id` (`chat_id`),
    INDEX `idx_send_time` (`send_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天记录表';
"""


async def init_db():
    """Initialize database connection pool and create table if not exists."""
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
        maxsize=10,
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(CREATE_TABLE_SQL)
            await cur.execute(CREATE_LOGIN_LOG_SQL)
            await cur.execute(CREATE_CONTACT_TABLE_SQL)
            await cur.execute(CREATE_MESSAGE_TABLE_SQL)
            await cur.execute(CREATE_IMPORT_BATCH_SQL)
            await cur.execute(CREATE_IMPORT_ACCOUNT_SQL)
    logger.info("Database initialized")


async def close_db():
    """Close database connection pool."""
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()


async def upsert_account(phone: str, api_id: int = None, api_hash: str = None,
                         tg_user_id: int = None, nickname: str = None,
                         username: str = None, status: str = "online",
                         country: str = None, device_model: str = None,
                         system_version: str = None, app_version: str = None,
                         lang_code: str = None, system_lang_code: str = None):
    """Insert or update account record."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = f"""
                INSERT INTO `{TABLE_NAME}` (phone, api_id, api_hash, tg_user_id, nickname, username, status,
                    country, device_model, system_version, app_version, lang_code, system_lang_code, last_login_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    api_id = VALUES(api_id),
                    api_hash = VALUES(api_hash),
                    tg_user_id = VALUES(tg_user_id),
                    nickname = VALUES(nickname),
                    username = VALUES(username),
                    status = VALUES(status),
                    country = VALUES(country),
                    device_model = VALUES(device_model),
                    system_version = VALUES(system_version),
                    app_version = VALUES(app_version),
                    lang_code = VALUES(lang_code),
                    system_lang_code = VALUES(system_lang_code),
                    last_login_time = VALUES(last_login_time)
            """
            await cur.execute(sql, (phone, api_id, api_hash, tg_user_id, nickname,
                                    username, status, country, device_model,
                                    system_version, app_version, lang_code,
                                    system_lang_code, datetime.now()))


async def update_account_status(phone: str, status: str):
    """Update account status."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE `{TABLE_NAME}` SET status = %s WHERE phone = %s",
                (status, phone)
            )


async def increment_msg_count(account_id: int, is_outgoing: bool):
    """Increment message count for an account."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if is_outgoing:
                    await cur.execute(
                        f"UPDATE `{TABLE_NAME}` SET total_msg_count = IFNULL(total_msg_count,0)+1, "
                        f"sent_msg_count = IFNULL(sent_msg_count,0)+1 WHERE id = %s",
                        (account_id,)
                    )
                else:
                    await cur.execute(
                        f"UPDATE `{TABLE_NAME}` SET total_msg_count = IFNULL(total_msg_count,0)+1, "
                        f"recv_msg_count = IFNULL(recv_msg_count,0)+1 WHERE id = %s",
                        (account_id,)
                    )
    except Exception as e:
        logger.error(f"Failed to increment msg count for account {account_id}: {e}")


async def increment_contact_msg_count(account_id: int, user_id: int, is_outgoing: bool):
    """Increment message count for a contact (friend-level stats)."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if is_outgoing:
                    await cur.execute(
                        "UPDATE tg_contact SET total_msg_count = IFNULL(total_msg_count,0)+1, "
                        "account_sent_count = IFNULL(account_sent_count,0)+1 "
                        "WHERE tg_account_id = %s AND user_id = %s",
                        (account_id, user_id)
                    )
                else:
                    await cur.execute(
                        "UPDATE tg_contact SET total_msg_count = IFNULL(total_msg_count,0)+1, "
                        "friend_sent_count = IFNULL(friend_sent_count,0)+1 "
                        "WHERE tg_account_id = %s AND user_id = %s",
                        (account_id, user_id)
                    )
    except Exception as e:
        logger.error(f"Failed to increment contact msg count for account {account_id}, user {user_id}: {e}")


async def get_all_accounts():
    """Get all accounts."""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM `{TABLE_NAME}` ORDER BY id")
            return await cur.fetchall()


async def get_account_by_phone(phone: str):
    """Get account by phone number."""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT * FROM `{TABLE_NAME}` WHERE phone = %s", (phone,)
            )
            return await cur.fetchone()


async def get_waiting_phones_by_batch(batch_no: str) -> list[str]:
    """Get phone numbers of waiting accounts for a specific batch."""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT phone FROM `{TABLE_NAME}` WHERE batch_no = %s AND status IN ('waiting', 'offline', 'failed')",
                (batch_no,)
            )
            rows = await cur.fetchall()
            return [r['phone'] for r in rows]


async def get_message_by_file_id(tg_account_id: int, file_id):
    """Get a message record by media_file_id to find chat_id and message_id."""
    try:
        file_id_int = int(file_id)
    except (ValueError, TypeError):
        return None
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT chat_id, message_id FROM `tg_chat_message` WHERE tg_account_id = %s AND media_file_id = %s LIMIT 1",
                (tg_account_id, file_id_int)
            )
            return await cur.fetchone()


async def insert_login_log(phone: str, result: str, reason: str = None,
                           tg_user_id: int = None, nickname: str = None,
                           proxy_info: str = None):
    """Insert a login log record.

    Args:
        phone: Account phone number.
        result: Login result - 'success', 'failed', or 'banned'.
        reason: Failure reason (optional).
        tg_user_id: Telegram user ID (on success).
        nickname: User nickname (on success).
        proxy_info: Proxy address used for login (optional).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = f"""
                INSERT INTO `{LOGIN_LOG_TABLE}` (phone, result, reason, tg_user_id, nickname, proxy_info, login_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            await cur.execute(sql, (phone, result, reason, tg_user_id, nickname, proxy_info, datetime.now()))


async def insert_send_fail_log(phone: str, tg_account_id: int = None,
                              nickname: str = None, user_id: int = None,
                              friend_nickname: str = None, friend_phone: str = None,
                              content_type: str = 'text', content: str = None,
                              error_reason: str = None):
    """Insert a send failure log record."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
                INSERT INTO tg_send_fail_log
                (phone, tg_account_id, nickname, user_id, friend_nickname,
                 friend_phone, content_type, content, error_reason, send_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            await cur.execute(sql, (phone, tg_account_id, nickname, user_id,
                                    friend_nickname, friend_phone, content_type,
                                    content, error_reason, datetime.now()))


async def insert_auto_reply_log(account_phone: str, account_nickname: str = None,
                               friend_user_id: int = None, friend_nickname: str = None,
                               friend_phone: str = None, trigger_type: str = None,
                               state: int = None, request_params: str = None,
                               chat_context: str = None, reply_content: str = None,
                               send_result: str = None, error_reason: str = None):
    """Insert an auto-reply log record."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = """
                    INSERT INTO tg_auto_reply_log
                    (account_phone, account_nickname, friend_user_id, friend_nickname,
                     friend_phone, trigger_type, state, request_params, chat_context,
                     reply_content, send_result, error_reason, create_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cur.execute(sql, (account_phone, account_nickname, friend_user_id,
                                        friend_nickname, friend_phone, trigger_type,
                                        state, request_params,
                                        chat_context[:2000] if chat_context else None,
                                        reply_content[:2000] if reply_content else None,
                                        send_result,
                                        error_reason[:500] if error_reason else None,
                                        datetime.now()))
    except Exception as e:
        logger.error(f"写入自动回复日志失败: {e}")


async def update_import_account_status(phone: str, status: str, reason: str = None,
                                        tg_user_id: int = None, nickname: str = None,
                                        username: str = None):
    """Update tg_import_account status after login attempt.
    Finds the most recent 'waiting' record for the phone and updates it.
    Then recalculates the batch counts."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # Find the most recent waiting import account record for this phone
                await cur.execute(
                    "SELECT id, batch_no FROM tg_import_account WHERE phone = %s AND status = 'waiting' ORDER BY id DESC LIMIT 1",
                    (phone,)
                )
                row = await cur.fetchone()
                if not row:
                    return

                import_id = row["id"]
                batch_no = row["batch_no"]

                # Update the import account record
                await cur.execute(
                    "UPDATE tg_import_account SET status = %s, reason = %s, tg_user_id = %s, "
                    "nickname = %s, username = %s, login_time = %s, update_time = NOW() WHERE id = %s",
                    (status, reason, tg_user_id, nickname, username, datetime.now(), import_id)
                )

                # Recalculate batch counts
                await cur.execute(
                    "SELECT "
                    "  SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END) AS success_count, "
                    "  SUM(CASE WHEN status IN ('failed', 'banned') THEN 1 ELSE 0 END) AS failed_count, "
                    "  SUM(CASE WHEN status = 'waiting' THEN 1 ELSE 0 END) AS waiting_count "
                    "FROM tg_import_account WHERE batch_no = %s",
                    (batch_no,)
                )
                counts = await cur.fetchone()
                if counts:
                    await cur.execute(
                        "UPDATE tg_import_batch SET success_count = %s, failed_count = %s, "
                        "waiting_count = %s, update_time = NOW() WHERE batch_no = %s",
                        (counts["success_count"] or 0, counts["failed_count"] or 0,
                         counts["waiting_count"] or 0, batch_no)
                    )
                logger.info(f"Updated import account {phone} to {status} (batch: {batch_no})")
    except Exception as e:
        logger.error(f"Failed to update import account status for {phone}: {e}")


async def save_device_fingerprint(phone: str, fingerprint: dict):
    """Save device fingerprint to the account record in database."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""UPDATE `{TABLE_NAME}` SET
                        device_model = %s,
                        system_version = %s,
                        app_version = %s,
                        lang_code = %s,
                        system_lang_code = %s,
                        update_time = NOW()
                    WHERE phone = %s""",
                    (
                        fingerprint.get('device_model'),
                        fingerprint.get('system_version'),
                        fingerprint.get('app_version'),
                        fingerprint.get('lang_code'),
                        fingerprint.get('system_lang_code'),
                        phone,
                    )
                )
                if cur.rowcount == 0:
                    # Account doesn't exist yet, insert with fingerprint
                    await cur.execute(
                        f"""INSERT INTO `{TABLE_NAME}` (phone, device_model, system_version, app_version, lang_code, system_lang_code)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                device_model = VALUES(device_model),
                                system_version = VALUES(system_version),
                                app_version = VALUES(app_version),
                                lang_code = VALUES(lang_code),
                                system_lang_code = VALUES(system_lang_code)""",
                        (
                            phone,
                            fingerprint.get('device_model'),
                            fingerprint.get('system_version'),
                            fingerprint.get('app_version'),
                            fingerprint.get('lang_code'),
                            fingerprint.get('system_lang_code'),
                        )
                    )
                logger.info(f"Saved device fingerprint for {phone}: {fingerprint.get('device_model')}")
    except Exception as e:
        logger.error(f"Failed to save device fingerprint for {phone}: {e}")
