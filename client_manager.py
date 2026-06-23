"""Telegram client manager for tg-gc-server (cluster node).

Key cluster changes:
- login_poll_loop: polls DB every 15s for login1/login2 accounts assigned to this node
- concurrent_login_accounts: logs in up to 15 accounts concurrently
- Session/JSON files recovered from DB before login
- All queries scoped to this node's node_id
"""
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
import node_manager
from phone_country import get_country_by_phone

logger = logging.getLogger(__name__)

# Active clients: phone -> TelegramClient
active_clients: dict[str, TelegramClient] = {}


def _get_account_dir(phone: str) -> str:
    """Get account directory: data/account/{phone}/"""
    d = os.path.join(config.ACCOUNT_DIR, phone)
    os.makedirs(d, exist_ok=True)
    return d


def _get_session_path(phone: str) -> str:
    """Get session path for Telethon (without .session extension)."""
    return os.path.join(_get_account_dir(phone), phone)


async def _recover_files_from_db(phone: str):
    """Recover .session and .json files from database to local disk if not present."""
    account_dir = _get_account_dir(phone)
    session_file = os.path.join(account_dir, phone + ".session")
    json_file = os.path.join(account_dir, phone + ".json")

    account = await database.get_account_by_phone(phone)
    if not account:
        return

    # Recover session file (binary)
    if not os.path.exists(session_file) and account.get("session_content"):
        try:
            with open(session_file, "wb") as f:
                f.write(account["session_content"])
            logger.info(f"[{phone}] Recovered .session file from DB")
        except Exception as e:
            logger.error(f"[{phone}] Failed to recover .session file: {e}")

    # Recover json file
    if not os.path.exists(json_file) and account.get("json_content"):
        try:
            with open(json_file, "w", encoding="utf-8") as f:
                f.write(account["json_content"])
            logger.info(f"[{phone}] Recovered .json file from DB")
        except Exception as e:
            logger.error(f"[{phone}] Failed to recover .json file: {e}")


def _read_account_json(phone: str) -> dict | None:
    """Read account JSON file."""
    json_path = os.path.join(_get_account_dir(phone), phone + ".json")
    if not os.path.exists(json_path):
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception as e:
        logger.error(f"Failed to read {json_path}: {e}")
        return None


def _has_session_file(phone: str) -> bool:
    """Check if .session file exists."""
    session_path = os.path.join(_get_account_dir(phone), phone + ".session")
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
    """Generate a random device fingerprint."""
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
        {"device_model": "iPhone 14 Pro", "system_version": "iOS 17.0"},
        {"device_model": "iPhone 15", "system_version": "iOS 17.1"},
        {"device_model": "Samsung Galaxy S24", "system_version": "Android 14"},
    ]
    device = random.choice(devices)
    app_versions = ["10.2.4", "10.3.1", "10.4.0", "10.5.2", "10.6.1"]
    lang_codes = ["en", "zh", "ko", "ja", "vi", "th"]
    return {
        "device_model": device["device_model"],
        "system_version": device["system_version"],
        "app_version": random.choice(app_versions),
        "lang_code": random.choice(lang_codes),
        "system_lang_code": "en-US",
    }


def get_active_phones() -> list[str]:
    """Get list of active phone numbers."""
    return list(active_clients.keys())


# ---------------------------------------------------------------------------
# Login polling loop (replaces old startup login)
# ---------------------------------------------------------------------------

async def login_poll_loop():
    """Background task: every 15s, check for login1/login2 accounts and login them."""
    while True:
        try:
            await asyncio.sleep(config.LOGIN_POLL_INTERVAL)
            await _process_pending_logins()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Login poll error: {e}")


async def _process_pending_logins():
    """Find accounts with status login1/login2 for this node and login them."""
    node_id = node_manager.NODE_ID

    # Get login1 accounts (with proxy)
    login1_accounts = await database.get_accounts_by_node_and_status(node_id, "login1")
    # Get login2 accounts (without proxy)
    login2_accounts = await database.get_accounts_by_node_and_status(node_id, "login2")

    if not login1_accounts and not login2_accounts:
        return

    results = []

    if login1_accounts:
        logger.info(f"Login poll: found {len(login1_accounts)} login1 accounts")
        r = await concurrent_login_accounts(login1_accounts, use_proxy=True)
        results.extend(r)

    if login2_accounts:
        logger.info(f"Login poll: found {len(login2_accounts)} login2 accounts")
        r = await concurrent_login_accounts(login2_accounts, use_proxy=False)
        results.extend(r)

    if results:
        online_count = sum(1 for r in results if r.get("success"))
        total_online = len(active_clients)
        await notify.send_notification(
            "登录完成",
            f"本次登录: {online_count}/{len(results)} 成功\n"
            f"节点在线总数: {total_online}"
        )


# ---------------------------------------------------------------------------
# Concurrent login
# ---------------------------------------------------------------------------

async def concurrent_login_accounts(accounts: list[dict], use_proxy: bool) -> list[dict]:
    """Login multiple accounts concurrently, up to CONCURRENT_LOGIN_LIMIT at a time."""
    semaphore = asyncio.Semaphore(config.CONCURRENT_LOGIN_LIMIT)
    results = []

    overall_timeout = config.LOGIN_TIMEOUT * 2

    async def _login_one(acc):
        async with semaphore:
            phone = acc["phone"]
            try:
                result = await asyncio.wait_for(
                    login_account_by_phone(phone, use_proxy=use_proxy),
                    timeout=overall_timeout,
                )
                results.append(result)
            except asyncio.TimeoutError:
                logger.error(f"[{phone}] Login overall timeout ({overall_timeout}s)")
                try:
                    await database.update_account_status(phone, "failed")
                    await database.insert_login_log(
                        phone=phone, result="failed",
                        reason=f"整体登录超时({overall_timeout}s)",
                        node_id=node_manager.NODE_ID,
                    )
                except Exception:
                    pass
                results.append({"phone": phone, "success": False, "error": f"Overall timeout ({overall_timeout}s)"})
            except Exception as e:
                logger.error(f"[{phone}] Concurrent login error: {e}")
                results.append({"phone": phone, "success": False, "error": str(e)})

    tasks = [asyncio.create_task(_login_one(acc)) for acc in accounts]
    await asyncio.gather(*tasks, return_exceptions=True)
    return results


# ---------------------------------------------------------------------------
# Single account login
# ---------------------------------------------------------------------------

async def login_account_by_phone(phone: str, use_proxy: bool = True) -> dict:
    """Login a single account by phone number.

    Args:
        phone: Phone number
        use_proxy: If True, use proxy from DB. If False (login2), skip proxy.
    """
    if phone in active_clients:
        return {"phone": phone, "success": True, "message": "Already online"}

    node_id = node_manager.NODE_ID

    # Recover files from DB if needed
    await _recover_files_from_db(phone)

    if not _has_session_file(phone):
        # No session file - cannot login
        await database.update_account_status(phone, "failed")
        await database.insert_login_log(
            phone=phone, result="failed", reason="无session文件",
            node_id=node_id
        )
        return {"phone": phone, "success": False, "error": "No session file"}

    # Determine api_id, api_hash, device kwargs
    json_data = _read_account_json(phone)
    api_id = 2040
    api_hash = "b18441a1ff607e10a989891a5462e627"
    device_kwargs = {}

    if json_data:
        api_id = json_data.get("app_id") or json_data.get("api_id") or api_id
        api_hash = json_data.get("app_hash") or json_data.get("api_hash") or api_hash
        device_kwargs = _build_device_kwargs(json_data)
    else:
        # Try loading from DB
        account = await database.get_account_by_phone(phone)
        if account and account.get("device_model"):
            device_kwargs = {
                "device_model": account["device_model"],
                "system_version": account.get("system_version") or "Android 13",
                "app_version": account.get("app_version") or "10.2.4",
                "lang_code": account.get("lang_code") or "en",
                "system_lang_code": account.get("system_lang_code") or "en-US",
            }
        else:
            device_kwargs = _generate_random_fingerprint()
            # Save to DB
            await database.save_device_fingerprint(phone, device_kwargs)

    # Build proxy kwargs
    proxy_kwargs = {}
    if use_proxy:
        account = await database.get_account_by_phone(phone)
        if account and account.get("proxy_url"):
            proxy_kwargs = _parse_proxy(account)
        elif use_proxy:
            # login1 but no proxy → fail
            await database.update_account_status(phone, "failed")
            await database.insert_login_log(
                phone=phone, result="failed", reason="login1但无代理配置",
                node_id=node_id
            )
            return {"phone": phone, "success": False, "error": "No proxy configured for login1"}

    # Create client
    session_path = _get_session_path(phone)
    login_timeout = config.LOGIN_TIMEOUT
    try:
        client = TelegramClient(
            session_path,
            api_id,
            api_hash,
            proxy=proxy_kwargs.get("proxy") if proxy_kwargs else None,
            **device_kwargs,
        )
        try:
            await asyncio.wait_for(client.connect(), timeout=login_timeout)
        except asyncio.TimeoutError:
            try:
                await client.disconnect()
            except Exception:
                pass
            await database.update_account_status(phone, "failed")
            await database.insert_login_log(
                phone=phone, result="failed", reason=f"连接超时({login_timeout}s)",
                node_id=node_id
            )
            logger.warning(f"[{phone}] Login timeout: connect exceeded {login_timeout}s")
            return {"phone": phone, "success": False, "error": f"Connect timeout ({login_timeout}s)"}

        try:
            authorized = await asyncio.wait_for(client.is_user_authorized(), timeout=login_timeout)
        except asyncio.TimeoutError:
            try:
                await client.disconnect()
            except Exception:
                pass
            await database.update_account_status(phone, "failed")
            await database.insert_login_log(
                phone=phone, result="failed", reason=f"授权检查超时({login_timeout}s)",
                node_id=node_id
            )
            logger.warning(f"[{phone}] Login timeout: is_user_authorized exceeded {login_timeout}s")
            return {"phone": phone, "success": False, "error": f"Auth check timeout ({login_timeout}s)"}

        if not authorized:
            await client.disconnect()
            await database.update_account_status(phone, "failed")
            proxy_info = proxy_kwargs.get("proxy_url", "") if proxy_kwargs else "无代理"
            await database.insert_login_log(
                phone=phone, result="failed", reason="Session未授权",
                proxy_info=proxy_info, node_id=node_id
            )
            return {"phone": phone, "success": False, "error": "Session not authorized"}

        try:
            me = await asyncio.wait_for(client.get_me(), timeout=login_timeout)
        except asyncio.TimeoutError:
            try:
                await client.disconnect()
            except Exception:
                pass
            await database.update_account_status(phone, "failed")
            await database.insert_login_log(
                phone=phone, result="failed", reason=f"get_me超时({login_timeout}s)",
                node_id=node_id
            )
            logger.warning(f"[{phone}] Login timeout: get_me exceeded {login_timeout}s")
            return {"phone": phone, "success": False, "error": f"get_me timeout ({login_timeout}s)"}
        nickname = ""
        if me:
            parts = [me.first_name or "", me.last_name or ""]
            nickname = " ".join(p for p in parts if p)

        # Register message event handler
        handler = _create_message_handler(phone)
        client.add_event_handler(handler, events.NewMessage(incoming=True))

        active_clients[phone] = client

        # Update DB
        country = get_country_by_phone(phone)
        await database.update_account_on_login(
            phone=phone,
            tg_user_id=me.id if me else None,
            nickname=nickname,
            username=me.username if me else None,
            country=country,
            device_kwargs=device_kwargs,
            node_id=node_id,
        )

        proxy_info = proxy_kwargs.get("proxy_url", "无代理")
        await database.insert_login_log(
            phone=phone, result="success",
            tg_user_id=me.id if me else None,
            nickname=nickname,
            proxy_info=proxy_info,
            node_id=node_id,
        )

        logger.info(f"[{phone}] Login success: {nickname} (@{me.username if me else ''})")

        # Trigger data sync in background
        asyncio.create_task(_safe_sync(phone))

        return {"phone": phone, "success": True, "nickname": nickname}

    except (AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
        await database.update_account_status(phone, "banned")
        await database.insert_login_log(
            phone=phone, result="banned", reason=str(e),
            node_id=node_id
        )
        return {"phone": phone, "success": False, "error": f"Banned: {e}"}
    except Exception as e:
        await database.update_account_status(phone, "failed")
        await database.insert_login_log(
            phone=phone, result="failed", reason=str(e),
            node_id=node_id
        )
        logger.error(f"[{phone}] Login failed: {e}")
        return {"phone": phone, "success": False, "error": str(e)}


def _parse_proxy(account: dict) -> dict:
    """Parse proxy config from account record."""
    proxy_url = account.get("proxy_url", "")
    protocol = (account.get("proxy_protocol") or "socks5").lower()
    host = account.get("proxy_host", "")
    port = account.get("proxy_port")
    username = account.get("proxy_username")
    password = account.get("proxy_password")

    if not host or not port:
        return {}

    import socks
    proxy_type_map = {
        "socks5": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
    }
    proxy_type = proxy_type_map.get(protocol, socks.SOCKS5)
    proxy = (proxy_type, host, int(port), True, username, password)

    return {"proxy": proxy, "proxy_url": proxy_url}


def _create_message_handler(phone: str):
    """Create a message event handler for a specific account."""
    async def handler(event):
        try:
            await _base_new_message_handler(phone, event)
        except Exception as e:
            logger.error(f"[{phone}] Message handler error: {e}")
    return handler


async def _base_new_message_handler(phone: str, event):
    """Handle incoming message: mark read, save to DB, trigger auto-reply."""
    msg = event.message
    if not msg or msg.out:
        return

    client = active_clients.get(phone)
    if not client:
        return

    # Mark as read
    try:
        await client.send_read_acknowledge(event.chat_id)
    except Exception:
        pass

    # Save message to DB (await to ensure it's saved before auto-reply)
    await data_collector.save_realtime_message(phone, event, client)

    # Trigger auto-reply (async, don't block)
    asyncio.create_task(auto_reply.handle_incoming_message(phone, event, client))


async def _safe_sync(phone: str):
    """Safely sync data for an account."""
    try:
        await data_collector.sync_account_data(phone)
    except Exception as e:
        logger.error(f"[{phone}] Data sync error: {e}")


# ---------------------------------------------------------------------------
# Logout / disconnect
# ---------------------------------------------------------------------------

async def logout_account(phone: str) -> dict:
    """Logout a specific account."""
    client = active_clients.pop(phone, None)
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    await database.update_account_status(phone, "offline")
    await database.insert_login_log(
        phone=phone, result="logout", reason="手动登出",
        node_id=node_manager.NODE_ID,
    )
    return {"phone": phone, "success": True}


async def disconnect_all():
    """Disconnect all active clients (for shutdown)."""
    phones = list(active_clients.keys())
    for phone in phones:
        client = active_clients.pop(phone, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass


async def sync_all_accounts():
    """Sync data for all active accounts on this node."""
    for phone in list(active_clients.keys()):
        try:
            await data_collector.sync_account_data(phone)
        except Exception as e:
            logger.error(f"[{phone}] Sync error: {e}")
        await asyncio.sleep(0.5)
