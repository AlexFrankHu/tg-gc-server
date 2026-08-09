"""Data collector for tg-gc-server (cluster node).

Syncs contacts and chat history from Telegram to database.
All operations scoped to this node.
"""
import asyncio
import logging
from datetime import datetime

from telethon.tl.types import User, UserStatusRecently, UserStatusOnline

import config
import database
import client_manager
import node_manager

logger = logging.getLogger(__name__)


async def sync_account_data(phone: str):
    """Sync contacts and recent messages for an account."""
    client = client_manager.active_clients.get(phone)
    if not client:
        return

    account = await database.get_account_by_phone(phone)
    if not account:
        return

    account_id = account["id"]
    node_id = node_manager.NODE_ID

    try:
        await _sync_contacts(client, phone, account_id, node_id)
    except Exception as e:
        logger.error(f"[{phone}] Contact sync error: {e}")

    try:
        await _sync_messages(client, phone, account_id, node_id)
    except Exception as e:
        logger.error(f"[{phone}] Message sync error: {e}")


async def _sync_contacts(client, phone: str, account_id: int, node_id: str):
    """Sync contacts/dialogs for an account."""
    try:
        dialogs = await client.get_dialogs(limit=100)
    except Exception as e:
        logger.warning(f"[{phone}] get_dialogs failed: {e}")
        return

    for dialog in dialogs:
        entity = dialog.entity
        if not isinstance(entity, User):
            continue
        if entity.bot or entity.deleted:
            user_type = "bot" if entity.bot else "deleted"
        else:
            user_type = "regular"

        nickname = ""
        parts = [entity.first_name or "", entity.last_name or ""]
        nickname = " ".join(p for p in parts if p)

        last_online = None
        if isinstance(entity.status, UserStatusOnline):
            last_online = datetime.now()
        elif isinstance(entity.status, UserStatusRecently):
            last_online = datetime.now()

        await database.upsert_contact(
            tg_account_id=account_id,
            user_id=entity.id,
            access_hash=getattr(entity, "access_hash", None),
            first_name=entity.first_name,
            last_name=entity.last_name,
            nickname=nickname,
            username=entity.username,
            phone_number=entity.phone,
            is_mutual=getattr(entity, "mutual_contact", False) or False,
            is_bot=entity.bot or False,
            is_premium=getattr(entity, "premium", False) or False,
            user_type=user_type,
            source="natural",
            node_id=node_id,
        )

    logger.info(f"[{phone}] Synced {len(dialogs)} dialog contacts")


async def _sync_messages(client, phone: str, account_id: int, node_id: str):
    """Sync recent messages for an account's contacts."""
    try:
        dialogs = await client.get_dialogs(limit=50)
    except Exception as e:
        logger.warning(f"[{phone}] get_dialogs for messages failed: {e}")
        return

    total_messages = 0
    total_sent = 0
    total_recv = 0

    for dialog in dialogs:
        entity = dialog.entity
        if not isinstance(entity, User):
            continue

        chat_id = entity.id
        last_msg_id = await database.get_latest_message_id(account_id, chat_id)

        try:
            if last_msg_id:
                messages = await client.get_messages(chat_id, min_id=last_msg_id, limit=100)
            else:
                messages = await client.get_messages(chat_id, limit=50)
        except Exception:
            continue

        msg_count = 0
        sent_count = 0
        recv_count = 0

        for msg in messages:
            if not msg or not msg.id:
                continue

            content_type = "text"
            text = msg.text or ""
            media_file_id = None
            media_file_size = None
            media_mime_type = None

            if msg.media:
                if hasattr(msg.media, "photo") and msg.media.photo:
                    content_type = "photo"
                    media_file_id = msg.media.photo.id
                elif hasattr(msg.media, "document") and msg.media.document:
                    doc = msg.media.document
                    mime = doc.mime_type or "application/octet-stream"
                    media_file_id = doc.id
                    media_file_size = doc.size
                    media_mime_type = mime
                    if "video" in mime:
                        content_type = "video"
                    elif "audio" in mime or "voice" in mime:
                        content_type = "voice"
                    else:
                        content_type = "document"
            elif msg.text:
                content_type = "text"
            else:
                content_type = "other"

            is_outgoing = msg.out or False
            send_time = config.to_beijing(msg.date) if msg.date else datetime.now()

            await database.insert_chat_message(
                tg_account_id=account_id,
                chat_id=chat_id,
                message_id=msg.id,
                sender_user_id=msg.sender_id,
                is_outgoing=is_outgoing,
                send_time=send_time,
                content_type=content_type,
                text_content=text[:5000] if text else None,
                media_file_id=media_file_id,
                media_file_size=media_file_size,
                media_mime_type=media_mime_type,
                node_id=node_id,
            )

            msg_count += 1
            if is_outgoing:
                sent_count += 1
            else:
                recv_count += 1

        if msg_count > 0:
            total_messages += msg_count
            total_sent += sent_count
            total_recv += recv_count

            # Update contact timestamps
            if sent_count > 0 or recv_count > 0:
                await database.update_contact_timestamps(
                    account_id, chat_id,
                    last_send_time=datetime.now() if sent_count > 0 else None,
                    last_receive_time=datetime.now() if recv_count > 0 else None,
                )
                await database.update_contact_msg_counts(
                    account_id, chat_id, msg_count, sent_count, recv_count
                )

        await asyncio.sleep(0.1)

    # Update account-level counts
    if total_messages > 0:
        await database.update_account_msg_counts(phone, total_messages, total_sent, total_recv)

    logger.info(f"[{phone}] Synced messages: +{total_messages} (sent:{total_sent}, recv:{total_recv})")


async def save_realtime_message(phone: str, event, client):
    """Save a real-time incoming message to DB."""
    try:
        account = await database.get_account_by_phone(phone)
        if not account:
            return

        account_id = account["id"]
        node_id = node_manager.NODE_ID
        msg = event.message

        content_type = "text"
        text = msg.text or ""
        media_file_id = None
        media_file_size = None
        media_mime_type = None

        if msg.media:
            if hasattr(msg.media, "photo") and msg.media.photo:
                content_type = "photo"
                media_file_id = msg.media.photo.id
            elif hasattr(msg.media, "document") and msg.media.document:
                doc = msg.media.document
                mime = doc.mime_type or "application/octet-stream"
                media_file_id = doc.id
                media_file_size = doc.size
                media_mime_type = mime
                if "video" in mime:
                    content_type = "video"
                elif "audio" in mime or "voice" in mime:
                    content_type = "voice"
                else:
                    content_type = "document"

        send_time = config.to_beijing(msg.date) if msg.date else datetime.now()

        await database.insert_chat_message(
            tg_account_id=account_id,
            chat_id=event.chat_id,
            message_id=msg.id,
            sender_user_id=msg.sender_id,
            is_outgoing=False,
            send_time=send_time,
            content_type=content_type,
            text_content=text[:5000] if text else None,
            media_file_id=media_file_id,
            media_file_size=media_file_size,
            media_mime_type=media_mime_type,
            node_id=node_id,
        )

        # Update contact receive timestamp
        await database.update_contact_timestamps(
            account_id, event.chat_id,
            last_receive_time=datetime.now(),
        )

    except Exception as e:
        logger.error(f"[{phone}] Save realtime message error: {e}")
