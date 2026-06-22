"""Contact adder module - polls pending assign logs and adds contacts via Telethon."""
import asyncio
import logging
import os
import random
from datetime import datetime, timezone, timedelta

import aiohttp
import aiomysql
from telethon.tl.functions.contacts import ImportContactsRequest, GetContactsRequest
from telethon.tl.types import InputPhoneContact

import database
import client_manager

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60  # seconds
MAX_RETRY_COUNT = 30

# Network-related error keywords that should trigger retry
NETWORK_ERROR_KEYWORDS = [
    'timeout', 'timed out', 'connection', 'network', 'unreachable',
    'reset', 'refused', 'broken pipe', 'eof', 'disconnect',
    'flood', 'floodwait', 'server error', 'internal error',
    'temporarily unavailable', 'could not connect',
]


def _is_network_error(error_msg: str) -> bool:
    """Check if an error message indicates a network/connectivity issue."""
    lower = error_msg.lower()
    return any(kw in lower for kw in NETWORK_ERROR_KEYWORDS)


async def poll_contact_adder():
    """Background task: poll pending contact assign logs every POLL_INTERVAL seconds."""
    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        try:
            await _process_pending_logs()
        except asyncio.CancelledError:
            logger.warning("[ContactAdder] CancelledError during processing, will retry next cycle")
            continue
        except Exception as e:
            logger.error(f"[ContactAdder] poll error: {e}")


async def _process_pending_logs():
    """Fetch pending assign logs and try to add contacts."""
    pending_logs = await _get_pending_logs()
    if not pending_logs:
        return

    logger.info(f"[ContactAdder] Found {len(pending_logs)} pending assign logs")
    affected_batch_nos = set()

    for log_entry in pending_logs:
        try:
            log_id = log_entry['id']
            account_phone = log_entry['account_phone']
            account_id = log_entry['account_id']
            contact_phone = log_entry.get('contact_phone')
            contact_username = log_entry.get('contact_username')
            contact_batch_no = log_entry.get('contact_batch_no')
            retry_count = log_entry.get('retry_count') or 0

            # Determine if this is a username-based or phone-based add
            is_username = bool(contact_username and not contact_phone)
            contact_display = contact_username if is_username else contact_phone

            # Check if account is online and connected
            if account_phone not in client_manager.active_clients:
                continue

            client = client_manager.active_clients[account_phone]

            # Try to reconnect if client is disconnected
            if not client.is_connected():
                logger.warning(f"[ContactAdder] log_id={log_id}: {account_phone} 已断开连接，尝试重连")
                try:
                    await asyncio.wait_for(client.connect(), timeout=15)
                    logger.info(f"[ContactAdder] {account_phone} 重连成功")
                except Exception as e:
                    logger.warning(f"[ContactAdder] {account_phone} 重连失败: {e}，跳过")
                    continue

            logger.info(f"[ContactAdder] 处理 log_id={log_id}: {account_phone} -> {contact_display} (retry={retry_count}, username={is_username})")

            # Try to add contact with timeout to prevent hanging
            try:
                if is_username:
                    await asyncio.wait_for(
                        _add_by_username(client, log_id, account_id, contact_username, retry_count, account_phone),
                        timeout=60
                    )
                else:
                    await asyncio.wait_for(
                        _add_by_phone(client, log_id, account_id, contact_phone, retry_count, account_phone),
                        timeout=60
                    )

            except asyncio.TimeoutError:
                new_retry = retry_count + 1
                logger.error(f"[ContactAdder] log_id={log_id}: 操作超时(60s)")
                await _update_log(log_id, 'pending', '操作超时将重试', new_retry)

            except asyncio.CancelledError:
                logger.warning(f"[ContactAdder] log_id={log_id}: CancelledError，跳过")
                continue

            except Exception as e:
                error_msg = str(e)
                new_retry = retry_count + 1
                logger.error(f"[ContactAdder] log_id={log_id}: 添加异常: {error_msg}")

                if _is_network_error(error_msg) and new_retry < MAX_RETRY_COUNT:
                    await _update_log(log_id, 'pending', f'网络异常将重试: {error_msg[:200]}', new_retry)
                elif new_retry >= MAX_RETRY_COUNT:
                    await _update_log(log_id, 'failed', f'超过最大重试次数({MAX_RETRY_COUNT}): {error_msg[:200]}', new_retry)
                else:
                    await _update_log(log_id, 'failed', error_msg[:300], new_retry)

            if contact_batch_no:
                affected_batch_nos.add(contact_batch_no)

            await asyncio.sleep(2)  # rate-limit between add operations

        except asyncio.CancelledError:
            logger.warning(f"[ContactAdder] CancelledError in log processing loop, continuing")
            continue
        except Exception as e:
            logger.error(f"[ContactAdder] 处理 log_id={log_entry.get('id')} 异常: {e}")

    # Refresh stats for all affected contact batches
    for batch_no in affected_batch_nos:
        await _refresh_batch_stats(batch_no)


async def _add_by_phone(client, log_id, account_id, contact_phone, retry_count, account_phone=''):
    """Add contact by phone number using ImportContactsRequest."""
    normalized_phone = contact_phone.strip()
    if not normalized_phone.startswith("+"):
        normalized_phone = "+" + normalized_phone

    # Check if already a contact
    already_friend = False
    user_id = None
    try:
        entity = await client.get_entity(normalized_phone)
        if entity:
            result = await client(GetContactsRequest(hash=0))
            phone_clean = normalized_phone.replace("+", "")
            for user in result.users:
                if user.phone and user.phone.replace("+", "") == phone_clean:
                    already_friend = True
                    user_id = user.id
                    break
    except Exception:
        pass

    if already_friend:
        logger.info(f"[ContactAdder] log_id={log_id}: 已是好友, user_id={user_id}")
        await _update_log(log_id, 'skipped', '已是好友', retry_count + 1)
        if user_id:
            await _ensure_contact_record(account_id, user_id, contact_phone)
        return

    # Import contact
    input_contact = InputPhoneContact(
        client_id=random.randint(0, 2**31),
        phone=normalized_phone,
        first_name=normalized_phone,
        last_name=""
    )
    result = await client(ImportContactsRequest([input_contact]))
    logger.info(f"[ContactAdder] log_id={log_id}: ImportContacts结果: imported={len(result.imported)}, users={len(result.users)}, retry_contacts={result.retry_contacts}")

    if result.imported:
        user = result.users[0] if result.users else None
        user_id = user.id if user else None
        logger.info(f"[ContactAdder] log_id={log_id}: 添加成功, user_id={user_id}")
        await _update_log(log_id, 'success', '添加成功', retry_count + 1)
        if user_id:
            await _ensure_contact_record(account_id, user_id, contact_phone)
            # await _send_greeting_to_new_friend(client, account_id, user_id, account_phone)  # 暂时去掉
    elif result.users:
        user_id = result.users[0].id
        logger.info(f"[ContactAdder] log_id={log_id}: 已是好友, user_id={user_id}")
        await _update_log(log_id, 'skipped', '已是好友', retry_count + 1)
        if user_id:
            await _ensure_contact_record(account_id, user_id, contact_phone)
    elif result.retry_contacts:
        logger.warning(f"[ContactAdder] log_id={log_id}: ImportContacts被限制, retry_contacts={result.retry_contacts}, 尝试通过搜索添加")
        # retry_contacts means user exists but import was rate-limited, try fallback
        fallback_ok = await _fallback_add_by_phone(client, log_id, account_id, normalized_phone, contact_phone, retry_count, account_phone)
        if not fallback_ok:
            await _update_log(log_id, 'pending', f'ImportContacts被限制,搜索也失败,将重试', retry_count + 1)
    else:
        # Fallback: try to find the user by phone and add via AddContactRequest
        logger.warning(f"[ContactAdder] log_id={log_id}: ImportContacts返回空, 尝试通过手机号搜索用户")
        fallback_ok = await _fallback_add_by_phone(client, log_id, account_id, normalized_phone, contact_phone, retry_count, account_phone)
        if not fallback_ok:
            await _update_log(log_id, 'failed', '该号码未注册Telegram或无法添加', retry_count + 1)


async def _fallback_add_by_phone(client, log_id, account_id, normalized_phone, contact_phone, retry_count, account_phone=''):
    """Fallback: try ResolvePhone or get_entity to find user, then AddContactRequest."""
    from telethon.tl.functions.contacts import AddContactRequest, ResolvePhoneRequest
    from telethon.tl.types import InputUser

    user = None

    # Method 1: Try ResolvePhoneRequest (Telegram layer 160+)
    try:
        phone_clean = normalized_phone.replace("+", "")
        resolved = await client(ResolvePhoneRequest(phone=phone_clean))
        if resolved and resolved.users:
            user = resolved.users[0]
            logger.info(f"[ContactAdder] log_id={log_id}: ResolvePhone找到用户 user_id={user.id}")
    except Exception as e:
        logger.info(f"[ContactAdder] log_id={log_id}: ResolvePhone失败: {e}")

    # Method 2: Try get_entity with phone number
    if not user:
        try:
            entity = await client.get_entity(normalized_phone)
            if entity:
                user = entity
                logger.info(f"[ContactAdder] log_id={log_id}: get_entity找到用户 user_id={user.id}")
        except Exception as e:
            logger.info(f"[ContactAdder] log_id={log_id}: get_entity失败: {e}")

    if not user:
        logger.warning(f"[ContactAdder] log_id={log_id}: 所有方式都无法找到用户 {normalized_phone}")
        return False

    # Found the user, now add as contact via AddContactRequest
    try:
        input_user = InputUser(user_id=user.id, access_hash=user.access_hash)
        await client(AddContactRequest(
            id=input_user,
            first_name=user.first_name or normalized_phone,
            last_name=user.last_name or "",
            phone=normalized_phone,
            add_phone_privacy_exception=True
        ))
        logger.info(f"[ContactAdder] log_id={log_id}: AddContactRequest成功, user_id={user.id}")
        await _update_log(log_id, 'success', f'通过搜索添加成功(user_id={user.id})', retry_count + 1)
        await _ensure_contact_record(account_id, user.id, contact_phone)
        # await _send_greeting_to_new_friend(client, account_id, user.id, account_phone)  # 暂时去掉
        return True
    except Exception as e:
        logger.error(f"[ContactAdder] log_id={log_id}: AddContactRequest失败: {e}")
        raise  # let outer handler deal with it


async def _add_by_username(client, log_id, account_id, contact_username, retry_count, account_phone=''):
    """Add contact by username using get_entity + send_message or AddContactRequest."""
    username = contact_username.strip()
    if username.startswith("@"):
        username = username[1:]

    # Try to resolve the username
    try:
        entity = await client.get_entity(username)
    except Exception as e:
        error_msg = str(e).lower()
        if 'no user has' in error_msg or 'cannot find' in error_msg or 'nobody is using' in error_msg:
            logger.warning(f"[ContactAdder] log_id={log_id}: 用户名 @{username} 不存在")
            await _update_log(log_id, 'failed', f'用户名 @{username} 不存在', retry_count + 1)
            return
        raise  # re-raise for network error handling

    if not entity:
        await _update_log(log_id, 'failed', f'无法解析用户名 @{username}', retry_count + 1)
        return

    user_id = entity.id

    # Check if already a friend
    already_friend = False
    try:
        result = await client(GetContactsRequest(hash=0))
        for user in result.users:
            if user.id == user_id:
                already_friend = True
                break
    except Exception:
        pass

    if already_friend:
        logger.info(f"[ContactAdder] log_id={log_id}: @{username} 已是好友, user_id={user_id}")
        await _update_log(log_id, 'skipped', '已是好友', retry_count + 1)
        await _ensure_contact_record(account_id, user_id, username)
        return

    # Add as contact using AddContactRequest
    from telethon.tl.functions.contacts import AddContactRequest
    from telethon.tl.types import InputUser
    try:
        await client(AddContactRequest(
            id=entity,
            first_name=getattr(entity, 'first_name', '') or username,
            last_name=getattr(entity, 'last_name', '') or '',
            phone='',
            add_phone_privacy_exception=False
        ))
        logger.info(f"[ContactAdder] log_id={log_id}: @{username} 添加成功, user_id={user_id}")
        await _update_log(log_id, 'success', '添加成功', retry_count + 1)
        await _ensure_contact_record(account_id, user_id, username)
        # await _send_greeting_to_new_friend(client, account_id, user_id, account_phone)  # 暂时去掉
    except Exception as e:
        error_msg = str(e)
        logger.error(f"[ContactAdder] log_id={log_id}: AddContact @{username} 失败: {error_msg}")
        raise  # let outer handler deal with it


async def _get_pending_logs() -> list:
    """Get all pending assign logs where retry_count < MAX_RETRY_COUNT."""
    async with database.pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM tg_contact_assign_log "
                "WHERE status = 'pending' AND (retry_count IS NULL OR retry_count < %s) "
                "ORDER BY id ASC",
                (MAX_RETRY_COUNT,),
            )
            return await cur.fetchall()


async def _update_log(log_id: int, status: str, remark: str, retry_count: int):
    """Update a contact assign log entry."""
    async with database.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE tg_contact_assign_log SET status = %s, remark = %s, retry_count = %s WHERE id = %s",
                (status, remark, retry_count, log_id),
            )


async def _ensure_contact_record(account_id: int, user_id: int, phone: str):
    """Ensure a tg_contact record exists for this account+user."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT id FROM tg_contact WHERE tg_account_id = %s AND user_id = %s",
                    (account_id, user_id),
                )
                existing = await cur.fetchone()
                if not existing:
                    await cur.execute(
                        """INSERT INTO tg_contact (tg_account_id, user_id, first_name, nickname,
                           phone_number, is_mutual, is_bot, user_type, auto_reply, source, create_time)
                           VALUES (%s, %s, %s, %s, %s, 0, 0, 'regular', 1, 'import', NOW())""",
                        (account_id, user_id, phone, phone, phone),
                    )
                    logger.info(f"[ContactAdder] tg_contact 已创建: account_id={account_id}, user_id={user_id}")
                else:
                    await cur.execute(
                        "UPDATE tg_contact SET source = 'import' WHERE id = %s AND source != 'import'",
                        (existing['id'],),
                    )
    except Exception as e:
        logger.error(f"[ContactAdder] 写入tg_contact失败: {e}")


async def batch_import_contacts(account_id: int, account_phone: str, import_type: str,
                                contact_batch_no: str, contacts: list) -> dict:
    """Batch import contacts using ImportContactsRequest for phone or get_entity+AddContactRequest for username.
    Returns dict with imported/failed/retry counts.
    """
    from telethon.tl.functions.contacts import AddContactRequest
    from telethon.tl.types import InputUser

    if account_phone not in client_manager.active_clients:
        return {"imported": 0, "failed": len(contacts), "error": "账号不在线"}

    client = client_manager.active_clients[account_phone]
    if not client.is_connected():
        try:
            await asyncio.wait_for(client.connect(), timeout=15)
        except Exception as e:
            return {"imported": 0, "failed": len(contacts), "error": f"重连失败: {e}"}

    imported_count = 0
    failed_count = 0
    retry_count = 0

    # Find assign log IDs for these contacts to update status
    async def _find_log_id(contact_value, is_username):
        try:
            async with database.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if is_username:
                        await cur.execute(
                            "SELECT id FROM tg_contact_assign_log WHERE account_id = %s AND contact_username = %s AND status = 'pending' ORDER BY id DESC LIMIT 1",
                            (account_id, contact_value),
                        )
                    else:
                        await cur.execute(
                            "SELECT id FROM tg_contact_assign_log WHERE account_id = %s AND contact_phone = %s AND status = 'pending' ORDER BY id DESC LIMIT 1",
                            (account_id, contact_value),
                        )
                    row = await cur.fetchone()
                    return row['id'] if row else None
        except Exception:
            return None

    if import_type == "username":
        # Username-based: add one by one using get_entity + AddContactRequest
        for c in contacts:
            username = c.get("username", "").strip()
            if username.startswith("@"):
                username = username[1:]
            if not username:
                failed_count += 1
                continue

            log_id = await _find_log_id(username, True)

            try:
                entity = await client.get_entity(username)
                if entity:
                    input_user = InputUser(user_id=entity.id, access_hash=entity.access_hash)
                    await client(AddContactRequest(
                        id=input_user,
                        first_name=entity.first_name or username,
                        last_name=entity.last_name or "",
                        phone="",
                        add_phone_privacy_exception=True
                    ))
                    imported_count += 1
                    logger.info(f"[BatchImport] {account_phone}: 添加用户名 {username} 成功, user_id={entity.id}")
                    if log_id:
                        await _update_log(log_id, 'success', '联系人导入-添加成功', 1)
                    await _ensure_contact_record(account_id, entity.id, username)
                    # await _send_greeting_to_new_friend(client, account_id, entity.id, account_phone)  # 暂时去掉
                else:
                    failed_count += 1
                    if log_id:
                        await _update_log(log_id, 'failed', '找不到该用户名', 1)
            except Exception as e:
                failed_count += 1
                err_msg = str(e)
                logger.warning(f"[BatchImport] {account_phone}: 添加用户名 {username} 失败: {err_msg}")
                if log_id:
                    await _update_log(log_id, 'failed', f'联系人导入失败: {err_msg[:200]}', 1)
            await asyncio.sleep(1)
    else:
        # Phone-based: batch import using ImportContactsRequest
        input_contacts = []
        phone_list = []
        for i, c in enumerate(contacts):
            phone = c.get("phone", "").strip()
            if not phone:
                failed_count += 1
                continue
            normalized = phone if phone.startswith("+") else "+" + phone
            input_contacts.append(InputPhoneContact(
                client_id=i,
                phone=normalized,
                first_name=normalized,
                last_name=""
            ))
            phone_list.append(phone)

        if not input_contacts:
            return {"imported": 0, "failed": failed_count, "retry": 0}

        try:
            result = await client(ImportContactsRequest(input_contacts))
            logger.info(f"[BatchImport] {account_phone}: ImportContacts批量结果: imported={len(result.imported)}, users={len(result.users)}, retry_contacts={len(result.retry_contacts)}")

            # Build mapping: client_id -> imported status
            imported_client_ids = set()
            for imp in result.imported:
                imported_client_ids.add(imp.client_id)

            # Build mapping: phone -> user
            phone_to_user = {}
            for user in result.users:
                if user.phone:
                    phone_clean = user.phone.replace("+", "")
                    phone_to_user[phone_clean] = user

            # Build retry set from client_ids
            retry_client_ids = set(result.retry_contacts)

            for i, phone in enumerate(phone_list):
                phone_clean = phone.replace("+", "")
                log_id = await _find_log_id(phone, False)

                if i in imported_client_ids:
                    # Successfully imported
                    user = phone_to_user.get(phone_clean)
                    user_id = user.id if user else None
                    imported_count += 1
                    logger.info(f"[BatchImport] {account_phone}: {phone} 导入成功, user_id={user_id}")
                    if log_id:
                        await _update_log(log_id, 'success', '联系人导入-添加成功', 1)
                    if user_id:
                        await _ensure_contact_record(account_id, user_id, phone)
                        # await _send_greeting_to_new_friend(client, account_id, user_id, account_phone)  # 暂时去掉
                elif phone_clean in phone_to_user:
                    # User exists (already a friend)
                    user = phone_to_user[phone_clean]
                    imported_count += 1
                    logger.info(f"[BatchImport] {account_phone}: {phone} 已是好友, user_id={user.id}")
                    if log_id:
                        await _update_log(log_id, 'skipped', '联系人导入-已是好友', 1)
                    await _ensure_contact_record(account_id, user.id, phone)
                elif i in retry_client_ids:
                    # Telegram wants retry later
                    retry_count += 1
                    logger.warning(f"[BatchImport] {account_phone}: {phone} 需要稍后重试(retry_contacts)")
                    if log_id:
                        await _update_log(log_id, 'pending', '联系人导入-Telegram要求稍后重试', 1)
                else:
                    # Not found / not registered
                    failed_count += 1
                    logger.warning(f"[BatchImport] {account_phone}: {phone} 未注册Telegram或无法添加")
                    if log_id:
                        await _update_log(log_id, 'failed', '联系人导入-该号码未注册Telegram或无法添加', 1)
        except Exception as e:
            err_msg = str(e)
            logger.error(f"[BatchImport] {account_phone}: ImportContacts批量异常: {err_msg}")
            # Mark all as failed
            for phone in phone_list:
                log_id = await _find_log_id(phone, False)
                if log_id:
                    await _update_log(log_id, 'failed', f'联系人导入异常: {err_msg[:200]}', 1)
            failed_count += len(phone_list)

    # Refresh batch stats
    if contact_batch_no:
        await _refresh_batch_stats(contact_batch_no)

    return {"imported": imported_count, "failed": failed_count, "retry": retry_count}


async def _refresh_batch_stats(batch_no: str):
    """Call Java API to refresh contact import batch statistics."""
    try:
        url = f"http://localhost:8809/tg/import/refreshContactBatchStats?batchNo={batch_no}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    logger.info(f"[ContactAdder] 已刷新批次统计: {batch_no}")
                else:
                    logger.warning(f"[ContactAdder] 刷新批次统计失败: {batch_no}, status={resp.status}")
    except Exception as e:
        logger.warning(f"[ContactAdder] 刷新批次统计异常: {batch_no}, {e}")


async def _send_greeting_to_new_friend(client, account_id: int, user_id: int, account_phone: str):
    """Send the first valid greeting to a newly added friend.
    account_phone is used for logging only."""
    try:
        # Get first enabled greeting from database
        async with database.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT id, content, image_path FROM tg_greeting WHERE is_enabled = 1 ORDER BY sort_order ASC, id ASC LIMIT 1"
                )
                greeting = await cur.fetchone()

        if not greeting:
            logger.info(f"[{account_phone}] [Greeting] 无有效问候语，跳过发送")
            return

        content = greeting.get('content', '')
        image_path = greeting.get('image_path', '')
        beijing_tz = timezone(timedelta(hours=8))

        if not content and not image_path:
            logger.info(f"[{account_phone}] [Greeting] 问候语内容为空，跳过")
            return

        sent_msg = None
        has_image = False

        # Send as one message: image+caption if has image, text only otherwise
        if image_path:
            actual_path = "/home/ubuntu/telegram-project/uploadPath" + image_path.replace("/profile", "", 1)
            if os.path.exists(actual_path):
                sent_msg = await client.send_file(user_id, actual_path, caption=content or '')
                has_image = True
                logger.info(f"[{account_phone}] [Greeting] 图片+文字问候语发送成功: user_id={user_id}")
            else:
                logger.warning(f"[{account_phone}] [Greeting] 图片不存在: {actual_path}, 仅发送文字")
                if content:
                    sent_msg = await client.send_message(user_id, content)
        else:
            if content:
                sent_msg = await client.send_message(user_id, content)
                logger.info(f"[{account_phone}] [Greeting] 文字问候语发送成功: user_id={user_id}")

        if not sent_msg:
            return

        # Save to database: split into separate records for text and image
        send_time = sent_msg.date.astimezone(beijing_tz) if sent_msg.date else datetime.now(beijing_tz)
        msg_id = sent_msg.id

        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Save text record
                if content:
                    await cur.execute(
                        """INSERT INTO tg_chat_message
                           (tg_account_id, chat_id, message_id, sender_user_id,
                            is_outgoing, send_time, content_type, text_content, create_time)
                           VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NOW())
                           ON DUPLICATE KEY UPDATE text_content = VALUES(text_content)""",
                        (account_id, user_id, msg_id, None,
                         send_time, 'text', content),
                    )
                    await database.increment_msg_count(account_id, is_outgoing=True)
                    await database.increment_contact_msg_count(account_id, user_id, is_outgoing=True)

                # Save image record
                if has_image:
                    img_msg_id = msg_id + 100000000 if content else msg_id
                    await cur.execute(
                        """INSERT INTO tg_chat_message
                           (tg_account_id, chat_id, message_id, sender_user_id,
                            is_outgoing, send_time, content_type, text_content, create_time)
                           VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NOW())
                           ON DUPLICATE KEY UPDATE text_content = VALUES(text_content)""",
                        (account_id, user_id, img_msg_id, None,
                         send_time, 'photo', image_path),
                    )
                    await database.increment_msg_count(account_id, is_outgoing=True)
                    await database.increment_contact_msg_count(account_id, user_id, is_outgoing=True)

                # Update last_send_time
                await cur.execute(
                    """UPDATE tg_contact SET last_send_time = %s
                       WHERE tg_account_id = %s AND user_id = %s""",
                    (send_time, account_id, user_id)
                )

        logger.info(f"[{account_phone}] [Greeting] 新好友问候语发送完成: user_id={user_id}")
    except Exception as e:
        logger.error(f"[{account_phone}] [Greeting] 发送问候语失败: user_id={user_id}, error={e}")
