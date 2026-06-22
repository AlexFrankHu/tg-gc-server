"""Main application entry point - tg-gc-server (cluster node)."""
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
import node_manager

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
    logger.info("Starting tg-gc-server (cluster node)...")

    # 1. Init database pool
    await database.init_db()

    # 2. Init node (load/generate node ID, register)
    await node_manager.init_node()
    logger.info(f"Node initialized: {node_manager.NODE_ID}")

    # 3. On restart: re-login previously online accounts for this node
    restart_task = asyncio.create_task(_restart_login())

    # 4. Start background tasks
    heartbeat_task = asyncio.create_task(node_manager.heartbeat_loop())
    login_poll_task = asyncio.create_task(client_manager.login_poll_loop())
    auto_reply_task = asyncio.create_task(auto_reply.poll_auto_reply())
    contact_adder_task = asyncio.create_task(contact_adder.poll_contact_adder())
    sync_task = asyncio.create_task(_periodic_sync())

    yield

    # Cancel all tasks
    for task in [heartbeat_task, login_poll_task, auto_reply_task,
                 contact_adder_task, sync_task, restart_task]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Shutdown
    logger.info("Shutting down...")
    await client_manager.disconnect_all()
    await database.close_db()
    logger.info("Shutdown complete")


async def _restart_login():
    """On restart, re-login accounts that were online before for this node."""
    try:
        # Write restart logout logs for accounts that were online
        accounts = await database.get_accounts_by_node_and_status(
            node_manager.NODE_ID, "online"
        )
        if accounts:
            logger.info(f"Restart: found {len(accounts)} previously online accounts")
            for acc in accounts:
                await database.insert_login_log(
                    phone=acc["phone"],
                    result="logout",
                    reason="服务重启",
                    tg_user_id=acc.get("tg_user_id"),
                    nickname=acc.get("nickname"),
                    proxy_info=acc.get("proxy_url"),
                    node_id=node_manager.NODE_ID,
                )

            # Filter: skip restricted, skip those without proxy
            to_login = [
                a for a in accounts
                if not a.get("is_restricted")
                and a.get("proxy_url")
            ]
            if to_login:
                logger.info(f"Restart: will re-login {len(to_login)} accounts (skipped restricted/no-proxy)")
                results = await client_manager.concurrent_login_accounts(
                    to_login, use_proxy=True
                )
                online_count = sum(1 for r in results if r.get("success"))
                await notify.send_notification(
                    "节点重启登录完成",
                    f"重新登录: {online_count}/{len(to_login)} 个在线"
                )
            else:
                logger.info("Restart: no accounts eligible for re-login")
        else:
            logger.info("Restart: no previously online accounts")
    except Exception as e:
        logger.error(f"Restart login error: {e}")


app = FastAPI(title="tg-gc-server", lifespan=lifespan)


@app.get("/api/status")
async def status():
    """Server status."""
    return {
        "status": "running",
        "node_id": node_manager.NODE_ID,
        "public_ip": node_manager.PUBLIC_IP,
        "active_count": len(client_manager.active_clients),
        "active_phones": client_manager.get_active_phones(),
    }


@app.post("/api/token")
async def get_token(account_id: int, phone: str = ""):
    """Generate an access token for the web client."""
    accounts = await database.get_all_accounts()
    account = next((a for a in accounts if a["id"] == account_id), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    token = auth.generate_token(account_id, account["phone"])
    return {"token": token, "account_id": account_id, "phone": account["phone"]}


@app.get("/api/accounts")
async def list_accounts():
    """List all accounts assigned to this node."""
    accounts = await database.get_accounts_by_node(node_manager.NODE_ID)
    for acc in accounts:
        for key in ["last_login_time", "create_time", "update_time"]:
            if acc.get(key):
                acc[key] = acc[key].strftime("%Y-%m-%d %H:%M:%S")
    return {"accounts": accounts, "node_id": node_manager.NODE_ID}


@app.get("/api/accounts/active")
async def list_active_accounts():
    """List currently active (online) accounts on this node."""
    return {
        "phones": client_manager.get_active_phones(),
        "count": len(client_manager.active_clients),
        "node_id": node_manager.NODE_ID,
    }


@app.post("/api/sync")
async def trigger_sync():
    """Manually trigger data sync for all active accounts on this node."""
    asyncio.create_task(client_manager.sync_all_accounts())
    return {"success": True, "message": "Sync triggered"}


@app.websocket("/ws/client")
async def websocket_route(websocket: WebSocket, token: str = Query(default=None)):
    """WebSocket endpoint for web client."""
    await ws_handler.websocket_endpoint(websocket, token)


@app.get("/api/client/tg/file")
async def download_file(tgAccountId: int, fileId: str, token: str = None,
                        chatId: int = None, messageId: int = None):
    """Download a media file from Telegram."""
    if not auth.validate_token(token):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    accounts = await database.get_all_accounts()
    account = next((a for a in accounts if a["id"] == tgAccountId), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    phone = account["phone"]
    if phone not in client_manager.active_clients:
        raise HTTPException(status_code=503, detail="Account not online on this node")

    client = client_manager.active_clients[phone]

    try:
        import io
        file_id = int(fileId)
        buffer = io.BytesIO()

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
    """Test a proxy IP."""
    import time
    import re
    import aiohttp
    import python_socks

    proxy_url = req.proxy_url.strip()
    result = {"proxy_url": proxy_url, "connected": False, "latency_ms": None,
              "real_ip": None, "geo": None, "error": None}

    try:
        pattern = r'^(socks5|socks4|http|https)://(?:([^:]+):([^@]+)@)?([^:]+):(\d+)$'
        m = re.match(pattern, proxy_url)
        if not m:
            result["error"] = "Invalid proxy URL format"
            return result

        protocol, username, password, host, port = m.group(1), m.group(2), m.group(3), m.group(4), int(m.group(5))

        from aiohttp_socks import ProxyConnector
        if protocol in ("socks5", "socks4"):
            proxy_type = python_socks.ProxyType.SOCKS5 if protocol == "socks5" else python_socks.ProxyType.SOCKS4
            connector = ProxyConnector(proxy_type=proxy_type, host=host, port=port,
                                       username=username, password=password)
        else:
            full_url = f"http://{host}:{port}"
            if username and password:
                full_url = f"http://{username}:{password}@{host}:{port}"
            connector = ProxyConnector.from_url(full_url)

        start = time.monotonic()
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get("http://httpbin.org/ip", timeout=aiohttp.ClientTimeout(total=15)) as resp:
                elapsed = time.monotonic() - start
                result["latency_ms"] = round(elapsed * 1000)
                result["connected"] = True
                data = await resp.json()
                result["real_ip"] = data.get("origin", "")

        if result["real_ip"]:
            try:
                ip = result["real_ip"].split(",")[0].strip()
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://ip-api.com/json/{ip}?lang=zh-CN",
                                           timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        geo = await resp.json()
                        if geo.get("status") == "success":
                            result["geo"] = {
                                "country": geo.get("country", ""), "region": geo.get("regionName", ""),
                                "city": geo.get("city", ""), "isp": geo.get("isp", ""),
                            }
            except Exception:
                pass
    except Exception as e:
        result["error"] = str(e)
    return result


@app.post("/api/notify/test")
async def test_notify(title: str = "测试通知", content: str = "这是一条测试消息"):
    """Test notification."""
    await notify.send_notification(title, content)
    return {"success": True}


async def _periodic_sync():
    """Periodically sync contacts and history (every hour)."""
    while True:
        await asyncio.sleep(3600)
        try:
            logger.info("Starting periodic data sync...")
            await client_manager.sync_all_accounts()
            logger.info("Periodic data sync complete")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Periodic sync error: {e}")


if __name__ == "__main__":
    uvicorn.run("main:app", host=config.HOST, port=config.PORT, log_level="info")
