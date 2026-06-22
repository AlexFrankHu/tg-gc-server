"""Telegram Bot notification module."""
import httpx
import config
import logging

logger = logging.getLogger(__name__)


async def send_notification(title: str, content: str):
    """Send notification via Telegram Bot API."""
    text = f"<b>{title}</b>\n{content}"
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.BOT_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                logger.info(f"Notification sent: {title}")
            else:
                logger.warning(f"Notification failed: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.error(f"Notification error: {e}")
