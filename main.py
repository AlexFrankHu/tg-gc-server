"""Main application entry point - FastAPI server."""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket, Query, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import config
import database
import client_manager
import notify
import ws_handler
import auth
import auto_reply
import contact_adder

# Setup logging
os.makedirs(config.LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "app.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown."""
    # Startup
    logger.info("Starting tg-client-telethon...")
    await database.init_db()

    # Logout all previously online accounts (write logout logs for restart)
    await _logout_all_on_startup()

    # Auto-login accounts that were previously online
    results = await client_manager.login_all_db_accounts()
    online_count = sum(1 for r in results if r.get("success"))
    logger.info(f"Startup login complete: {online_count}/{len(results)} accounts online")

    if results:
        await notify.send_notification(
            "服务启动",
            f"tg-client-telethon 已启动\n登录账号: {online_count}/{len(results)} 个在线"
        )

    # Start periodic sync task (every hour)
    sync_task = asyncio.create_task(_periodic_sync())
    # Start auto-reply polling task
    auto_reply_task = asyncio.create_task(auto_reply.poll_auto_reply())
    # Start contact adder polling task
    contact_adder_task = asyncio.create_task(contact_adder.poll_contact_adder())

    yield

    # Cancel tasks
    contact_adder_task.cancel()
    auto_reply_task.cancel()
    sync_task.cancel()
    for task in [sync_task, auto_reply_task, contact_adder_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Shutdown
    logger.info("Shutting down...")
    await client_manager.disconnect_all()
    await database.close_db()
    logger.info("Shutdown complete")


async def _logout_all_on_startup():
    """On restart, write logout logs for all accounts that were previously online,
    then set their status back to 'online' so login_all_db_accounts picks them up."""
    try:
        accounts = await database.get_all_accounts()
        online_accounts = [a for a in accounts if a.get('status') == 'online']
        if not online_accounts:
            logger.info("Startup logout: no online accounts to log out")
            return
        logger.info(f"Startup logout: writing logout logs for {len(online_accounts)} accounts")
        for acc in online_accounts:
            phone = acc['phone']
            await database.insert_login_log(
                phone=phone, result='logout',
                reason='服务重启',
                tg_user_id=acc.get('tg_user_id'),
                nickname=acc.get('nickname'),
                proxy_info=acc.get('proxy_url')
            )
            logger.info(f"Startup logout log written for +{phone}")
        # Keep status='online' so login_all_db_accounts will re-login them
    except Exception as e:
        logger.error(f"Startup logout error: {e}")


app = FastAPI(title="tg-client-telethon", lifespan=lifespan)


@app.post("/api/token")
async def get_token(account_id: int, phone: str = ""):
    """Generate an access token for the web client."""
    # Verify account exists
    accounts = await database.get_all_accounts()
    account = next((a for a in accounts if a["id"] == account_id), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    token = auth.generate_token(account_id, account["phone"])
    return {"token": token, "account_id": account_id, "phone": account["phone"]}


@app.get("/api/status")
async def status():
    """Server status."""
    return {
        "status": "running",
        "active_accounts": client_manager.get_active_phones(),
        "active_count": len(client_manager.active_clients),
    }


@app.post("/api/login/batch/{batch_no}")
async def login_batch_accounts(batch_no: str):
    """Login all waiting accounts in a specific batch."""
    results = await client_manager.login_batch(batch_no)
    return {
        "total": len(results),
        "success": sum(1 for r in results if r.get("success")),
        "results": results,
    }


@app.post("/api/login/wait")
async def login_all_waiting():
    """Login all waiting/offline/failed accounts."""
    results = await client_manager.login_all_waiting_accounts()
    return {
        "total": len(results),
        "success": sum(1 for r in results if r.get("success")),
        "results": results,
    }


@app.post("/api/login/noproxy/{phone}")
async def login_single_account_no_proxy(phone: str):
    """Login a specific account without proxy (for testing purposes)."""
    result = await client_manager.login_account_by_phone(phone, no_proxy=True)
    return result


@app.post("/api/login/{phone}")
async def login_single_account(phone: str):
    """Login a specific account by phone number."""
    result = await client_manager.login_account_by_phone(phone)
    return result


@app.post("/api/logout/{phone}")
async def logout_account(phone: str):
    """Logout a specific account."""
    result = await client_manager.logout_account(phone)
    return result


@app.post("/api/logout/batch/{batch_no}")
async def logout_batch_accounts(batch_no: str):
    """Logout all online accounts in a specific batch."""
    results = await client_manager.logout_batch(batch_no)
    return {
        "total": len(results),
        "results": results,
    }


@app.post("/api/logout/all")
async def logout_all_online():
    """Logout all online accounts."""
    results = await client_manager.logout_all_online()
    return {
        "total": len(results),
        "results": results,
    }


@app.get("/api/accounts")
async def list_accounts():
    """List all accounts from database."""
    accounts = await database.get_all_accounts()
    # Convert datetime objects to string for JSON serialization
    for acc in accounts:
        for key in ["last_login_time", "create_time", "update_time"]:
            if acc.get(key):
                acc[key] = acc[key].strftime("%Y-%m-%d %H:%M:%S")
    return {"accounts": accounts}


@app.get("/api/accounts/active")
async def list_active_accounts():
    """List currently active (online) accounts."""
    return {
        "phones": client_manager.get_active_phones(),
        "count": len(client_manager.active_clients),
    }


@app.get("/api/accounts/wait")
async def list_wait_accounts():
    """List accounts waiting to login (from database)."""
    accounts = await database.get_all_accounts()
    waiting = [a for a in accounts if a.get("status") in ("waiting", "offline", "failed")]
    return {
        "accounts": [{"phone": a["phone"], "status": a.get("status")} for a in waiting],
        "count": len(waiting),
    }


@app.post("/api/sync")
async def trigger_sync():
    """Manually trigger data sync for all active accounts."""
    asyncio.create_task(client_manager.sync_all_accounts())
    return {"success": True, "message": "Sync triggered"}


class AddContactRequest(BaseModel):
    phone: str  # account phone
    contact_phone: str  # friend phone to add


@app.post("/api/add-contact")
async def add_contact(req: AddContactRequest):
    """Add a contact by phone number for a specific account."""
    client = client_manager.active_clients.get(req.phone)
    if not client:
        return {"success": False, "error": f"账号 {req.phone} 未登录"}

    try:
        from telethon.tl.functions.contacts import ImportContactsRequest, GetContactsRequest
        from telethon.tl.types import InputPhoneContact
        import random

        # Normalize contact phone: ensure + prefix
        contact_phone = req.contact_phone.strip()
        if not contact_phone.startswith("+"):
            contact_phone = "+" + contact_phone

        logger.info(f"[{req.phone}] Adding contact, original: {req.contact_phone}, normalized: {contact_phone}")

        # First check if already a contact by searching
        try:
            entity = await client.get_entity(contact_phone)
            if entity:
                # Check if already in contacts
                result = await client(GetContactsRequest(hash=0))
                contact_phone_clean = contact_phone.replace("+", "")
                for user in result.users:
                    if user.phone and user.phone.replace("+", "") == contact_phone_clean:
                        return {"success": True, "skipped": True, "message": "已是好友", "user_id": user.id}
        except Exception:
            pass

        # Import contact
        contact = InputPhoneContact(
            client_id=random.randint(0, 2**31),
            phone=contact_phone,
            first_name=contact_phone,
            last_name=""
        )
        result = await client(ImportContactsRequest([contact]))

        if result.imported:
            user = result.users[0] if result.users else None
            user_id = user.id if user else None
            logger.info(f"[{req.phone}] Added contact {contact_phone}, user_id={user_id}")
            return {"success": True, "skipped": False, "message": "添加成功", "user_id": user_id}
        elif result.users:
            user_id = result.users[0].id
            return {"success": True, "skipped": True, "message": "已是好友", "user_id": user_id}
        else:
            return {"success": False, "error": "该号码未注册Telegram或无法添加"}
    except Exception as e:
        logger.error(f"[{req.phone}] Add contact {contact_phone} error: {e}")
        return {"success": False, "error": str(e)}


@app.websocket("/ws/client")
async def websocket_route(websocket: WebSocket, token: str = Query(default=None)):
    """WebSocket endpoint for web client."""
    await ws_handler.websocket_endpoint(websocket, token)


@app.get("/api/client/tg/file")
async def download_file(tgAccountId: int, fileId: str, token: str = None,
                        chatId: int = None, messageId: int = None):
    """Download a media file from Telegram."""
    # Validate token
    if not auth.validate_token(token):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Find the account
    accounts = await database.get_all_accounts()
    account = next((a for a in accounts if a["id"] == tgAccountId), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    phone = account["phone"]
    if phone not in client_manager.active_clients:
        raise HTTPException(status_code=503, detail="Account not online")

    client = client_manager.active_clients[phone]

    try:
        import io
        file_id = int(fileId)
        buffer = io.BytesIO()

        # Method 1: If chatId and messageId provided, get message directly
        if chatId and messageId:
            msgs = await client.get_messages(chatId, ids=[messageId])
            if msgs and msgs[0] and msgs[0].media:
                msg = msgs[0]
                mime = "image/jpeg"
                if hasattr(msg.media, 'document') and msg.media.document:
                    mime = msg.media.document.mime_type or "application/octet-stream"
                await client.download_media(msg.media, buffer)
                buffer.seek(0)
                return StreamingResponse(buffer, media_type=mime)

        # Method 2: Look up chatId and messageId from database by fileId
        chat_msg = await database.get_message_by_file_id(tgAccountId, fileId)
        if chat_msg:
            try:
                msgs = await client.get_messages(chat_msg["chat_id"], ids=[chat_msg["message_id"]])
                if msgs and msgs[0] and msgs[0].media:
                    msg = msgs[0]
                    mime = "image/jpeg"
                    if hasattr(msg.media, 'document') and msg.media.document:
                        mime = msg.media.document.mime_type or "application/octet-stream"
                    await client.download_media(msg.media, buffer)
                    buffer.seek(0)
                    return StreamingResponse(buffer, media_type=mime)
            except Exception as e:
                logger.warning(f"Failed to download via DB lookup: {e}")

        # Method 3: Fallback - scan recent messages (limited scope)
        async for dialog in client.iter_dialogs(limit=20):
            async for msg in client.iter_messages(dialog.entity, limit=30):
                if msg.media:
                    if hasattr(msg.media, 'photo') and msg.media.photo and msg.media.photo.id == file_id:
                        await client.download_media(msg.media, buffer)
                        buffer.seek(0)
                        return StreamingResponse(buffer, media_type="image/jpeg")
                    if hasattr(msg.media, 'document') and msg.media.document and msg.media.document.id == file_id:
                        mime = msg.media.document.mime_type or "application/octet-stream"
                        await client.download_media(msg.media, buffer)
                        buffer.seek(0)
                        return StreamingResponse(buffer, media_type=mime)

        raise HTTPException(status_code=404, detail="File not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File download error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class ProxyTestRequest(BaseModel):
    proxy_url: str


@app.post("/api/proxy/test")
async def test_proxy(req: ProxyTestRequest):
    """Test a proxy IP - connectivity, latency, real IP, geo location."""
    import time
    import re
    import aiohttp
    import python_socks

    proxy_url = req.proxy_url.strip()
    result = {
        "proxy_url": proxy_url,
        "connected": False,
        "latency_ms": None,
        "real_ip": None,
        "geo": None,
        "error": None,
    }

    try:
        # Parse proxy URL
        pattern = r'^(socks5|socks4|http|https)://(?:([^:]+):([^@]+)@)?([^:]+):(\d+)$'
        m = re.match(pattern, proxy_url)
        if not m:
            result["error"] = "Invalid proxy URL format"
            return result

        protocol = m.group(1)
        username = m.group(2)
        password = m.group(3)
        host = m.group(4)
        port = int(m.group(5))

        # Build connector based on protocol
        from aiohttp_socks import ProxyConnector

        if protocol in ("socks5", "socks4"):
            proxy_type = python_socks.ProxyType.SOCKS5 if protocol == "socks5" else python_socks.ProxyType.SOCKS4
            connector = ProxyConnector(
                proxy_type=proxy_type,
                host=host,
                port=port,
                username=username,
                password=password,
            )
        else:
            full_url = f"http://{host}:{port}"
            if username and password:
                full_url = f"http://{username}:{password}@{host}:{port}"
            connector = ProxyConnector.from_url(full_url)

        # Test connectivity and measure latency
        start = time.monotonic()
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get("http://httpbin.org/ip", timeout=aiohttp.ClientTimeout(total=15)) as resp:
                elapsed = time.monotonic() - start
                result["latency_ms"] = round(elapsed * 1000)
                result["connected"] = True
                data = await resp.json()
                result["real_ip"] = data.get("origin", "")

        # Get geo info for the IP
        if result["real_ip"]:
            try:
                ip = result["real_ip"].split(",")[0].strip()
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://ip-api.com/json/{ip}?lang=zh-CN", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        geo = await resp.json()
                        if geo.get("status") == "success":
                            result["geo"] = {
                                "country": geo.get("country", ""),
                                "region": geo.get("regionName", ""),
                                "city": geo.get("city", ""),
                                "isp": geo.get("isp", ""),
                                "org": geo.get("org", ""),
                            }
            except Exception:
                pass

    except Exception as e:
        result["error"] = str(e)

    return result


class BatchImportRequest(BaseModel):
    accountId: int
    accountPhone: str
    importType: str = "phone"
    contactBatchNo: str = ""
    contacts: list = []


@app.post("/api/contacts/batch_import")
async def batch_import_contacts_api(req: BatchImportRequest):
    """Batch import contacts for an account using ImportContactsRequest."""
    result = await contact_adder.batch_import_contacts(
        account_id=req.accountId,
        account_phone=req.accountPhone,
        import_type=req.importType,
        contact_batch_no=req.contactBatchNo,
        contacts=[c if isinstance(c, dict) else c.dict() for c in req.contacts],
    )
    return result


@app.post("/api/send-greeting")
async def send_greeting(request: Request):
    """Send a greeting message (text + optional image) to a friend."""
    data = await request.json()
    account_id = data.get("accountId")
    user_id = data.get("userId")
    content = data.get("content", "")
    image_path = data.get("imagePath")  # relative path like /profile/upload/...

    if not account_id or not user_id:
        return {"success": False, "error": "accountId and userId are required"}

    # Find client by account_id
    accounts = await database.get_all_accounts()
    account = next((a for a in accounts if a["id"] == account_id), None)
    if not account:
        return {"success": False, "error": "账号不存在"}

    phone = account["phone"]
    client = client_manager.active_clients.get(phone)

    if not client or not client.is_connected():
        return {"success": False, "error": "账号未在线或未连接"}

    try:
        import os
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))

        if not content and not image_path:
            return {"success": False, "error": "问候语内容为空"}

        sent_msg = None
        has_image = False

        # Send: if has image, send as one message (image + caption); if text only, send text
        if image_path:
            actual_path = "/home/ubuntu/telegram-project/uploadPath" + image_path.replace("/profile", "", 1)
            if os.path.exists(actual_path):
                # Send image with text as caption (one message)
                sent_msg = await client.send_file(user_id, actual_path, caption=content or '')
                has_image = True
            else:
                logger.warning(f"[{phone}] 问候语图片不存在: {actual_path}, 仅发送文字")
                if content:
                    sent_msg = await client.send_message(user_id, content)
        else:
            if content:
                sent_msg = await client.send_message(user_id, content)

        if not sent_msg:
            return {"success": False, "error": "发送失败，无有效内容"}

        # Save to database: split into separate records for text and image
        try:
            send_time = sent_msg.date.astimezone(beijing_tz) if sent_msg.date else datetime.now(beijing_tz)
            msg_id = sent_msg.id

            async with database.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Save text record (if has text)
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

                    # Save image record (if has image)
                    if has_image:
                        # Use msg_id + 100000000 offset to avoid duplicate key with text record
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

                    # If only text (no image), still count once
                    if not has_image and not content:
                        pass  # shouldn't happen

                    # Update last_send_time
                    await cur.execute(
                        """UPDATE tg_contact SET last_send_time = %s
                           WHERE tg_account_id = %s AND user_id = %s""",
                        (send_time, account_id, user_id)
                    )
        except Exception as e:
            logger.error(f"[{phone}] 更新发送统计/录入聊天记录失败: {e}")

        return {"success": True, "message": "问候语发送成功"}
    except Exception as e:
        logger.error(f"[{phone}] 发送问候语失败: account_id={account_id}, user_id={user_id}, error={e}")
        return {"success": False, "error": str(e)}


@app.post("/api/notify/test")
async def test_notify(title: str = "测试通知", content: str = "这是一条测试消息"):
    """Test notification."""
    await notify.send_notification(title, content)
    return {"success": True, "message": "Notification sent"}


async def _periodic_sync():
    """Periodically sync contacts and history (every hour)."""
    while True:
        await asyncio.sleep(3600)  # 1 hour
        try:
            logger.info("Starting periodic data sync...")
            await client_manager.sync_all_accounts()
            logger.info("Periodic data sync complete")
        except Exception as e:
            logger.error(f"Periodic sync error: {e}")


if __name__ == "__main__":
    uvicorn.run("main:app", host=config.HOST, port=config.PORT, reload=False)
