"""Telegram bot notification - includes node info in all messages."""
import logging
import httpx
import config

logger = logging.getLogger(__name__)


async def send_notification(title: str, content: str):
    """Send a Telegram bot notification with node info."""
    import node_manager
    node_info = node_manager.get_node_info_str()
    text = f"📢 {title}\n{node_info}\n{content}"

    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={
                "chat_id": config.BOT_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
            })
            if resp.status_code != 200:
                logger.warning(f"TG notify failed: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.error(f"TG notify error: {e}")
