"""Telegram client manager using Telethon."""
import os
import json
import asyncio
import logging
import random
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    AuthKeyUnregisteredError, UserDeactivatedBanError,
)

import config
import database
import notify
import data_collector
import auto_reply
from phone_country import get_country_by_phone

logger = logging.getLogger(__name__)

# Active clients: phone -> TelegramClient
active_clients: dict[str, TelegramClient] = {}


def _read_account_json(phone: str) -> dict | None:
    """Read account JSON file from account/ directory."""
    json_path = os.path.join(config.ACCOUNT_DIR, phone + ".json")
    if not os.path.exists(json_path):
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception as e:
        logger.error(f"Failed to read {json_path}: {e}")
        return None


def _get_session_path(phone: str) -> str:
    """Get session path for Telethon (without .session extension)."""
    return os.path.join(config.ACCOUNT_DIR, phone)


def _has_account_files(phone: str) -> bool:
    """Check if account .session file exists (.json is optional)."""
    session_path = os.path.join(config.ACCOUNT_DIR, phone + ".session")
    return os.path.exists(session_path)


def _build_device_kwargs(data: dict) -> dict:
    """Extract device fingerprint parameters from account JSON data."""
    kwargs = {}
    device_model = data.get("device_model") or data.get("device")
    if device_model:
        kwargs["device_model"] = device_model
    if data.get("system_version"):
        kwargs["system_version"] = data["system_version"]
    if data.get("app_version"):
        kwargs["app_version"] = data["app_version"]
    if data.get("lang_pack"):
        kwargs["lang_code"] = data["lang_pack"]
    if data.get("system_lang_pack"):
        kwargs["system_lang_code"] = data["system_lang_pack"]
    return kwargs


def _generate_random_fingerprint() -> dict:
    """Generate a random device fingerprint for Telethon client."""
    devices = [
        {"device_model": "Samsung Galaxy S21", "system_version": "Android 12"},
        {"device_model": "Samsung Galaxy S22", "system_version": "Android 13"},
        {"device_model": "Samsung Galaxy S23", "system_version": "Android 14"},
        {"device_model": "Samsung Galaxy A54", "system_version": "Android 13"},
        {"device_model": "Xiaomi 13", "system_version": "Android 13"},
        {"device_model": "Xiaomi 14", "system_version": "Android 14"},
        {"device_model": "HUAWEI P60", "system_version": "Android 13"},
        {"device_model": "OPPO Find X6", "system_version": "Android 13"},
        {"device_model": "vivo X90", "system_version": "Android 13"},
        {"device_model": "OnePlus 11", "system_version": "Android 13"},
        {"device_model": "Google Pixel 7", "system_version": "Android 13"},
        {"device_model": "Google Pixel 8", "system_version": "Android 14"},
        {"device_model": "iPhone 14 Pro", "system_version": "iOS 16.5"},
        {"device_model": "iPhone 15", "system_version": "iOS 17.0"},
        {"device_model": "iPhone 13", "system_version": "iOS 16.3"},
    ]
    app_versions = ["10.1.2", "10.2.0", "10.3.1", "10.4.0", "10.5.0", "10.6.3", "10.7.0", "10.8.0"]
    lang_codes = ["en", "zh", "ru", "ar", "es", "pt", "vi", "th"]
    system_lang_codes = ["en-US", "zh-CN", "zh-TW", "ru-RU", "ar-SA", "es-ES", "pt-BR", "vi-VN", "th-TH"]

    device = random.choice(devices)
    kwargs = {
        "device_model": device["device_model"],
        "system_version": device["system_version"],
        "app_version": random.choice(app_versions),
        "lang_code": random.choice(lang_codes),
        "system_lang_code": random.choice(system_lang_codes),
    }
    return kwargs


def _build_proxy_kwargs(account_row: dict | None) -> dict:
    """Build proxy parameter for TelegramClient from DB account row."""
    if not account_row:
        return {}
    proxy_protocol = account_row.get("proxy_protocol")
    proxy_host = account_row.get("proxy_host")
    proxy_port = account_row.get("proxy_port")
    if not proxy_protocol or not proxy_host or not proxy_port:
        return {}

    import socks
    proto_map = {
        "socks5": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
    }
    proxy_type = proto_map.get(proxy_protocol.lower())
    if proxy_type is None:
        return {}

    proxy_username = account_row.get("proxy_username") or None
    proxy_password = account_row.get("proxy_password") or None

    return {
        "proxy": (proxy_type, proxy_host, int(proxy_port), True, proxy_username, proxy_password)
    }


async def login_account_by_phone(phone: str, no_proxy: bool = False) -> dict:
    """Login a single account by phone number.

    Reads .json and .session from account/ directory.
    On success, creates account/{phone}/ for cache files.
    If no_proxy=True, skip proxy requirement check and login without proxy.
    """
    # Check if already online
    if phone in active_clients:
        return {"phone": phone, "success": True, "message": "Already online"}

    # Check files exist
    if not _has_account_files(phone):
        msg = f"Account {phone}: missing .session file in account/"
        logger.error(msg)
        await database.insert_login_log(phone=phone, result="failed", reason="缺少 .session 文件", proxy_info=None)
        await database.update_account_status(phone, "failed")
        await database.update_import_account_status(phone, "failed", reason="缺少 .session 文件")
        return {"phone": phone, "success": False, "error": msg}

    # Read JSON (optional - may not exist for session-only imports)
    data = _read_account_json(phone)
    if data:
        api_id = data.get("app_id") or data.get("api_id")
        api_hash = data.get("app_hash") or data.get("api_hash")
        device_kwargs = _build_device_kwargs(data)
    else:
        api_id = None
        api_hash = None
        device_kwargs = {}
        # No .json file: try to load fingerprint from database
        account_row_fp = await database.get_account_by_phone(phone)
        if account_row_fp and account_row_fp.get('device_model'):
            device_kwargs = _build_device_kwargs({
                'device_model': account_row_fp.get('device_model'),
                'system_version': account_row_fp.get('system_version'),
                'app_version': account_row_fp.get('app_version'),
                'lang_pack': account_row_fp.get('lang_code'),
                'system_lang_pack': account_row_fp.get('system_lang_code'),
            })
            logger.info(f"Account {phone}: no .json file, loaded fingerprint from DB: {device_kwargs}")
        else:
            # Generate random fingerprint and save to DB
            device_kwargs = _generate_random_fingerprint()
            logger.info(f"Account {phone}: no .json file and no DB fingerprint, generated random: {device_kwargs}")
            await database.save_device_fingerprint(phone, device_kwargs)

    # Use default api_id/api_hash if not available from JSON
    if not api_id or not api_hash:
        # Default Telegram Desktop api_id/api_hash
        api_id = 2040
        api_hash = "b18441a1ff607e10a989891a5462e627"
        logger.info(f"Account {phone}: no .json file or missing api_id/api_hash, using default")

    session_path = _get_session_path(phone)
    # device_kwargs already set above

    # Get proxy info from database
    account_row = await database.get_account_by_phone(phone)
    proxy_kwargs = _build_proxy_kwargs(account_row)
    proxy_url = account_row.get("proxy_url") if account_row else None

    # Proxy is required for all login operations (unless no_proxy mode)
    if not no_proxy and not proxy_kwargs:
        msg = f"Account {phone}: 未配置代理IP，禁止登录"
        logger.warning(msg)
        await database.insert_login_log(phone=phone, result="failed", reason="未配置代理IP，禁止登录", proxy_info=None)
        await database.update_account_status(phone, "failed")
        await database.update_import_account_status(phone, "failed", reason="未配置代理IP，禁止登录")
        return {"phone": phone, "success": False, "error": msg}

    # In no_proxy mode, clear proxy settings
    if no_proxy:
        proxy_kwargs = {}
        proxy_url = None

    try:
        client = TelegramClient(session_path, api_id, api_hash, **device_kwargs, **proxy_kwargs)
        await client.connect()

        if not await client.is_user_authorized():
            msg = f"Account {phone}: session not authorized, cannot auto-login"
            logger.warning(msg)
            await client.disconnect()
            await database.insert_login_log(phone=phone, result="failed", reason="session 未授权，需要重新验证", proxy_info=proxy_url)
            await database.update_account_status(phone, "failed")
            await database.update_import_account_status(phone, "failed", reason="session 未授权，需要重新验证")
            await notify.send_notification("登录失败", f"账号 +{phone}\n原因: session 未授权，需要重新验证")
            return {"phone": phone, "success": False, "error": msg}

        # Get user info
        me = await client.get_me()
        nickname = " ".join(filter(None, [me.first_name, me.last_name]))
        username = me.username

        # Create cache directory for this account
        cache_dir = os.path.join(config.ACCOUNT_DIR, phone)
        os.makedirs(cache_dir, exist_ok=True)

        # Save to active clients
        active_clients[phone] = client

        # Register event handler for real-time messages
        @client.on(events.NewMessage)
        async def _base_new_message_handler(event):
            try:
                msg = event.message
                if msg and not msg.out:
                    logger.info(f"[{phone}] New incoming message from chat {event.chat_id}")
                    # Mark incoming message as read
                    try:
                        await client.send_read_acknowledge(event.chat_id, msg)
                    except Exception as e:
                        logger.warning(f"[{phone}] Failed to mark message as read: {e}")
                # Save message to DB first, then trigger auto-reply
                await data_collector.save_realtime_message(phone, event, client)
                # Trigger auto-reply for incoming private messages (after DB save)
                if msg and not msg.out:
                    asyncio.create_task(auto_reply.handle_incoming_message(phone, event, client))
            except Exception as e:
                logger.error(f"[{phone}] Base handler error: {e}")

        try:
            await client.catch_up()
        except Exception:
            pass

        # Update database with country and device fingerprint
        country = get_country_by_phone(phone)
        await database.upsert_account(
            phone=phone,
            api_id=api_id,
            api_hash=api_hash,
            tg_user_id=me.id,
            nickname=nickname,
            username=username,
            status="online",
            country=country,
            device_model=(data.get("device_model") or data.get("device")) if data else None,
            system_version=data.get("system_version") if data else None,
            app_version=data.get("app_version") if data else None,
            lang_code=data.get("lang_pack") if data else None,
            system_lang_code=data.get("system_lang_pack") if data else None,
        )

        logger.info(f"Account +{phone} logged in successfully (user_id={me.id}, nickname={nickname})")
        await database.insert_login_log(phone=phone, result="success", tg_user_id=me.id, nickname=nickname, proxy_info=proxy_url)
        await database.update_import_account_status(phone, "online", tg_user_id=me.id, nickname=nickname, username=username)
        await notify.send_notification(
            "登录成功",
            f"账号: +{phone}\n昵称: {nickname}\n用户名: @{username or '-'}\nUser ID: {me.id}"
        )

        # Trigger data collection in background
        asyncio.create_task(_run_data_sync(client, phone))

        return {
            "phone": phone,
            "success": True,
            "user_id": me.id,
            "nickname": nickname,
            "username": username,
        }

    except (AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
        error_msg = str(e)
        logger.error(f"Account +{phone} banned/deactivated: {error_msg}")
        await database.insert_login_log(phone=phone, result="banned", reason=error_msg, proxy_info=proxy_url)
        await database.update_account_status(phone, "banned")
        await database.update_import_account_status(phone, "banned", reason=error_msg)
        await notify.send_notification("账号已被注销", f"账号: +{phone}\n原因: {error_msg}")
        return {"phone": phone, "success": False, "error": error_msg}

    except (ConnectionError, OSError) as e:
        error_msg = str(e)
        is_proxy_error = proxy_url and ("proxy" in error_msg.lower() or "socks" in error_msg.lower()
                                        or "connection refused" in error_msg.lower()
                                        or "timed out" in error_msg.lower())
        if is_proxy_error:
            logger.error(f"Account +{phone} proxy connection failed: {error_msg}")
            await database.insert_login_log(phone=phone, result="failed", reason=f"代理连接失败: {error_msg}", proxy_info=proxy_url)
            await database.update_account_status(phone, "failed")
            await database.update_import_account_status(phone, "failed", reason=f"代理连接失败: {error_msg}")
            await notify.send_notification("代理连接失败", f"账号: +{phone}\n代理: {proxy_url}\n原因: {error_msg}")
            return {"phone": phone, "success": False, "error": f"代理连接失败: {error_msg}"}
        else:
            logger.error(f"Account +{phone} login failed: {error_msg}")
            await database.insert_login_log(phone=phone, result="failed", reason=error_msg, proxy_info=proxy_url)
            await database.update_account_status(phone, "failed")
            await database.update_import_account_status(phone, "failed", reason=error_msg)
            await notify.send_notification("登录失败", f"账号: +{phone}\n原因: {error_msg}")
            return {"phone": phone, "success": False, "error": error_msg}

    except Exception as e:
        error_msg = str(e)
        is_proxy_error = proxy_url and ("proxy" in error_msg.lower() or "socks" in error_msg.lower())
        if is_proxy_error:
            logger.error(f"Account +{phone} proxy error: {error_msg}")
            await database.insert_login_log(phone=phone, result="failed", reason=f"代理错误: {error_msg}", proxy_info=proxy_url)
            await database.update_account_status(phone, "failed")
            await database.update_import_account_status(phone, "failed", reason=f"代理错误: {error_msg}")
            await notify.send_notification("代理连接失败", f"账号: +{phone}\n代理: {proxy_url}\n原因: {error_msg}")
        else:
            logger.error(f"Account +{phone} login failed: {error_msg}")
            await database.insert_login_log(phone=phone, result="failed", reason=error_msg, proxy_info=proxy_url)
            await database.update_account_status(phone, "failed")
            await database.update_import_account_status(phone, "failed", reason=error_msg)
            await notify.send_notification("登录失败", f"账号: +{phone}\n原因: {error_msg}")
        return {"phone": phone, "success": False, "error": error_msg}


async def login_batch(batch_no: str) -> list[dict]:
    """Login all waiting accounts in a specific batch."""
    phones = await database.get_waiting_phones_by_batch(batch_no)
    if not phones:
        logger.info(f"No waiting accounts for batch {batch_no}")
        return []

    results = []
    for phone in phones:
        result = await login_account_by_phone(phone)
        results.append(result)
        await asyncio.sleep(1)

    return results


async def login_all_db_accounts() -> list[dict]:
    """Re-login all accounts that were online (used on startup).
    Reads accounts from database that have status 'online' and tries to log them in.
    Skips accounts marked as restricted (is_restricted=1)."""
    accounts = await database.get_all_accounts()
    online_accounts = [a for a in accounts if a.get("status") == "online"]
    if not online_accounts:
        logger.info("No previously online accounts to re-login")
        return []

    results = []
    for acc in online_accounts:
        phone = acc["phone"]
        if acc.get("is_restricted"):
            logger.info(f"Account {phone} is restricted, skipping auto-login")
            results.append({"phone": phone, "success": False, "error": "account restricted"})
            continue
        if _has_account_files(phone):
            result = await login_account_by_phone(phone)
            results.append(result)
            await asyncio.sleep(1)
        else:
            logger.warning(f"Account {phone} was online but files missing, setting offline")
            await database.update_account_status(phone, "offline")
            results.append({"phone": phone, "success": False, "error": "files missing"})

    return results


async def login_all_waiting_accounts() -> list[dict]:
    """Login all accounts with status waiting/offline/failed.
    Skips accounts marked as restricted (is_restricted=1)."""
    accounts = await database.get_all_accounts()
    waiting = [a for a in accounts if a.get("status") in ("waiting", "offline", "failed")]

    if not waiting:
        logger.info("No waiting accounts to login")
        return []

    results = []
    for acc in waiting:
        phone = acc["phone"]
        if phone in active_clients:
            continue
        if acc.get("is_restricted"):
            logger.info(f"Account {phone} is restricted, skipping login")
            results.append({"phone": phone, "success": False, "error": "account restricted"})
            continue
        if _has_account_files(phone):
            result = await login_account_by_phone(phone)
            results.append(result)
            await asyncio.sleep(1)
        else:
            logger.warning(f"Account {phone} has no files, skipping")
            results.append({"phone": phone, "success": False, "error": "files missing"})

    return results


async def logout_account(phone: str) -> dict:
    """Disconnect and mark account as offline, write logout log."""
    # Get account info for log before disconnecting
    account = await database.get_account_by_phone(phone)
    tg_user_id = account.get('tg_user_id') if account else None
    nickname = account.get('nickname') if account else None
    proxy_url = account.get('proxy_url') if account else None

    client = active_clients.pop(phone, None)
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass

    await database.update_account_status(phone, "offline")
    # Write logout log
    await database.insert_login_log(
        phone=phone, result='logout',
        tg_user_id=tg_user_id, nickname=nickname,
        proxy_info=proxy_url
    )
    logger.info(f"Account +{phone} logged out")
    return {"phone": phone, "status": "offline"}


async def logout_batch(batch_no: str) -> list[dict]:
    """Logout all online accounts in a specific batch."""
    accounts = await database.get_all_accounts()
    batch_accounts = [a for a in accounts if a.get("batch_no") == batch_no and a.get("status") == "online"]

    results = []
    for acc in batch_accounts:
        phone = acc["phone"]
        result = await logout_account(phone)
        results.append(result)

    return results


async def logout_all_online() -> list[dict]:
    """Logout all online accounts."""
    results = []
    for phone in list(active_clients.keys()):
        result = await logout_account(phone)
        results.append(result)

    return results


async def disconnect_all():
    """Disconnect all active clients on shutdown.
    Do NOT set status to offline — preserve 'online' status
    so accounts auto-login on next startup."""
    for phone, client in list(active_clients.items()):
        try:
            await client.disconnect()
        except Exception:
            pass
    active_clients.clear()
    logger.info("All clients disconnected (status preserved for auto-login)")


def get_active_phones() -> list[str]:
    """Get list of currently active account phones."""
    return list(active_clients.keys())


async def _run_data_sync(client, phone: str):
    """Run data sync in background, catching all errors."""
    try:
        await data_collector.sync_contacts_and_history(client, phone)
    except Exception as e:
        logger.error(f"[{phone}] Data sync failed: {e}")


async def sync_all_accounts():
    """Sync contacts and history for all active accounts (called by scheduler).
    Also checks if accounts are still connected and updates status if not."""
    for phone, client in list(active_clients.items()):
        try:
            if not client.is_connected():
                logger.warning(f"[{phone}] Client disconnected, updating status to offline")
                active_clients.pop(phone, None)
                await database.update_account_status(phone, "offline")
                continue

            try:
                me = await client.get_me()
                if not me:
                    logger.warning(f"[{phone}] Session no longer authorized, updating status")
                    active_clients.pop(phone, None)
                    await database.update_account_status(phone, "offline")
                    continue
            except (AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
                logger.warning(f"[{phone}] Account banned/deactivated: {e}")
                active_clients.pop(phone, None)
                await database.update_account_status(phone, "banned")
                await notify.send_notification("账号状态异常", f"账号: +{phone}\n原因: {e}")
                continue
            except Exception as e:
                logger.warning(f"[{phone}] Failed to verify account: {e}")
                active_clients.pop(phone, None)
                await database.update_account_status(phone, "offline")
                continue

            # Sync data
            await data_collector.sync_contacts_and_history(client, phone)
        except Exception as e:
            logger.error(f"[{phone}] Scheduled sync failed: {e}")
        await asyncio.sleep(2)
