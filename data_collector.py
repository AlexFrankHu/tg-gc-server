"""Data collection module - collects contacts and chat history."""
import logging
import asyncio
from datetime import datetime
from config import to_beijing
from telethon import functions
from telethon.tl.types import (
    User, UserStatusOnline, UserStatusOffline, UserStatusRecently,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage,
    MessageMediaGeo, MessageMediaGeoLive, MessageMediaContact,
    MessageMediaPoll, MessageMediaDice, MessageMediaInvoice,
    MessageMediaGame, MessageMediaStory,
    Document, Photo, MessageService
)
import database

logger = logging.getLogger(__name__)

CONTACT_TABLE = "tg_contact"
MESSAGE_TABLE = "tg_chat_message"


async def get_account_id(phone: str):
    """Get account ID from database by phone."""
    account = await database.get_account_by_phone(phone)
    if account:
        return account["id"]
    return None


async def sync_contacts_and_history(client, phone: str):
    """Sync contacts and chat history for an account."""
    account_id = await get_account_id(phone)
    if not account_id:
        logger.warning(f"Account {phone} not found in database, skip sync")
        return

    logger.info(f"[{phone}] Starting data sync...")

    # 1. Collect contacts
    try:
        result = await client(functions.contacts.GetContactsRequest(hash=0))
        contacts = getattr(result, "users", [])
        logger.info(f"[{phone}] Got {len(contacts)} contacts")
        for user in contacts:
            if isinstance(user, User):
                await upsert_contact(account_id, user)
    except Exception as e:
        logger.error(f"[{phone}] Failed to get contacts: {e}")

    # 2. Get imported contact user_ids for this account
    imported_user_ids = set()
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT user_id FROM tg_contact WHERE tg_account_id = %s AND source = 'import'",
                    (account_id,),
                )
                rows = await cur.fetchall()
                imported_user_ids = {r[0] for r in rows}
        logger.info(f"[{phone}] Found {len(imported_user_ids)} imported contacts")
    except Exception as e:
        logger.error(f"[{phone}] Failed to query imported contacts: {e}")

    # 3. Collect chats and history (only for imported contacts)
    try:
        dialogs = await client.get_dialogs(limit=None)
        logger.info(f"[{phone}] Got {len(dialogs)} dialogs (all chats)")
        for dialog in dialogs:
            if dialog.is_user:
                entity = dialog.entity
                if isinstance(entity, User):
                    await upsert_contact(account_id, entity)
                # Only collect history for imported contacts
                if dialog.entity.id in imported_user_ids:
                    await collect_chat_history(client, account_id, dialog.entity.id)
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.error(f"[{phone}] Failed to collect dialogs: {e}")

    logger.info(f"[{phone}] Data sync complete")


async def collect_chat_history(client, account_id: int, chat_id: int, limit: int = 500):
    """Collect chat history for a specific chat."""
    try:
        messages = await client.get_messages(chat_id, limit=limit)
        count = 0
        last_send_time = None
        last_receive_time = None
        for msg in messages:
            if isinstance(msg, MessageService):
                continue
            await upsert_message(account_id, chat_id, msg)
            count += 1
            # Track last send/receive times from history
            if msg.date:
                msg_date_bj = to_beijing(msg.date)
                if msg.out:
                    if last_send_time is None or msg_date_bj > last_send_time:
                        last_send_time = msg_date_bj
                else:
                    if last_receive_time is None or msg_date_bj > last_receive_time:
                        last_receive_time = msg_date_bj
        if count > 0:
            logger.debug(f"  Collected {count} messages for chat {chat_id}")
        # Update last_send_time / last_receive_time for the contact
        if last_send_time or last_receive_time:
            await update_contact_last_times(account_id, chat_id, last_send_time, last_receive_time)
    except Exception as e:
        logger.error(f"  Failed to collect history for chat {chat_id}: {e}")


async def upsert_contact(account_id: int, user: User):
    """Insert or update a contact record."""
    try:
        user_id = user.id
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        nickname = " ".join(filter(None, [first_name, last_name]))
        username = user.username
        phone_number = user.phone
        is_mutual = getattr(user, "mutual_contact", False)
        is_bot = user.bot or False
        is_premium = getattr(user, "premium", False) or False
        is_verified = getattr(user, "verified", False) or False

        if user.bot:
            user_type = "bot"
        elif user.deleted:
            user_type = "deleted"
        else:
            user_type = "regular"

        # Last online time
        last_online = None
        status = user.status
        if isinstance(status, UserStatusOnline):
            last_online = datetime.now()
        elif isinstance(status, UserStatusOffline):
            last_online = to_beijing(status.was_online)

        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = f"""
                    INSERT INTO `{CONTACT_TABLE}` (tg_account_id, user_id, first_name, last_name, nickname,
                        username, phone_number, is_mutual, is_bot, is_premium, is_verified, user_type,
                        last_online_time, update_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        first_name = VALUES(first_name),
                        last_name = VALUES(last_name),
                        nickname = VALUES(nickname),
                        username = VALUES(username),
                        phone_number = VALUES(phone_number),
                        is_mutual = VALUES(is_mutual),
                        is_bot = VALUES(is_bot),
                        is_premium = VALUES(is_premium),
                        is_verified = VALUES(is_verified),
                        user_type = VALUES(user_type),
                        last_online_time = VALUES(last_online_time),
                        update_time = NOW()
                """
                await cur.execute(sql, (account_id, user_id, first_name, last_name, nickname,
                                        username, phone_number, is_mutual, is_bot, is_premium,
                                        is_verified, user_type, last_online))
    except Exception as e:
        logger.error(f"Failed to upsert contact {user.id}: {e}")


async def update_contact_last_times(account_id: int, chat_id: int, last_send=None, last_receive=None):
    """Update last_send_time and/or last_receive_time for a contact.
    Only updates if the new time is more recent than the existing value."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                updates = []
                params = []
                if last_send is not None:
                    updates.append("last_send_time = CASE WHEN last_send_time IS NULL OR last_send_time < %s THEN %s ELSE last_send_time END")
                    params.extend([last_send, last_send])
                if last_receive is not None:
                    updates.append("last_receive_time = CASE WHEN last_receive_time IS NULL OR last_receive_time < %s THEN %s ELSE last_receive_time END")
                    params.extend([last_receive, last_receive])
                if not updates:
                    return
                sql = f"UPDATE `{CONTACT_TABLE}` SET {', '.join(updates)} WHERE tg_account_id = %s AND user_id = %s"
                params.extend([account_id, chat_id])
                await cur.execute(sql, params)
    except Exception as e:
        logger.error(f"Failed to update contact last times for account {account_id}, chat {chat_id}: {e}")


async def save_realtime_message(phone: str, event, client=None):
    """Save a real-time incoming/outgoing message to database.
    Also adds new chat sender to contact list if not already there."""
    try:
        account_id = await get_account_id(phone)
        if not account_id:
            return
        chat_id = event.chat_id
        msg = event.message
        if msg and not isinstance(msg, MessageService):
            await upsert_message(account_id, chat_id, msg)
            # Update last_send_time or last_receive_time
            if msg.date:
                msg_date_bj = to_beijing(msg.date)
                if msg.out:
                    await update_contact_last_times(account_id, chat_id, last_send=msg_date_bj, last_receive=None)
                else:
                    await update_contact_last_times(account_id, chat_id, last_send=None, last_receive=msg_date_bj)
            # Increment message count (exclude system accounts)
            if chat_id and chat_id != 777000:
                await database.increment_msg_count(account_id, is_outgoing=bool(msg.out))
                await database.increment_contact_msg_count(account_id, chat_id, is_outgoing=bool(msg.out))

        # If we have a client, try to add the sender as a contact record
        if client and msg and not msg.out:
            try:
                sender = await event.get_sender()
                if sender and isinstance(sender, User):
                    await upsert_contact(account_id, sender)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"[{phone}] save_realtime_message error: {e}")


async def upsert_message(account_id: int, chat_id: int, msg):
    """Insert a message record (skip if exists)."""
    try:
        message_id = msg.id
        sender_user_id = msg.sender_id.user_id if msg.sender_id and hasattr(msg.sender_id, "user_id") else None
        sender_chat_id = msg.sender_id.channel_id if msg.sender_id and hasattr(msg.sender_id, "channel_id") else None
        sender_name = None
        is_outgoing = msg.out or False
        send_time = to_beijing(msg.date)

        # Determine content type and extract media info
        content_type = "text"
        text_content = msg.text or msg.message or ""
        media_file_id = None
        media_file_size = None
        media_mime_type = None
        media_file_name = None
        media_duration = None
        media_width = None
        media_height = None
        thumbnail_file_id = None

        if msg.media:
            if isinstance(msg.media, MessageMediaPhoto):
                content_type = "photo"
                if msg.media.photo and isinstance(msg.media.photo, Photo):
                    media_file_id = msg.media.photo.id
            elif isinstance(msg.media, MessageMediaDocument):
                doc = msg.media.document
                if isinstance(doc, Document):
                    media_file_id = doc.id
                    media_file_size = doc.size
                    media_mime_type = doc.mime_type
                    is_animated = getattr(doc, 'mime_type', '') == 'application/x-tgsticker'
                    for attr in doc.attributes:
                        attr_type = type(attr).__name__
                        if attr_type == "DocumentAttributeFilename":
                            media_file_name = attr.file_name
                        elif attr_type == "DocumentAttributeVideo":
                            if getattr(attr, 'round_message', False):
                                content_type = "video_note"
                            else:
                                content_type = "video"
                            media_duration = attr.duration
                            media_width = attr.w
                            media_height = attr.h
                        elif attr_type == "DocumentAttributeAudio":
                            if attr.voice:
                                content_type = "voice"
                            else:
                                content_type = "audio"
                            media_duration = attr.duration
                        elif attr_type == "DocumentAttributeSticker":
                            content_type = "sticker"
                        elif attr_type == "DocumentAttributeAnimated":
                            content_type = "gif"
                        elif attr_type == "DocumentAttributeCustomEmoji":
                            content_type = "custom_emoji"
                    # If mime_type indicates GIF animation
                    if content_type == "text" and media_mime_type in ('image/gif', 'video/mp4') and 'animated' in str(doc.attributes).lower():
                        content_type = "gif"
                    if content_type == "text":
                        content_type = "document"
            elif isinstance(msg.media, MessageMediaWebPage):
                content_type = "text"
            elif isinstance(msg.media, MessageMediaGeo):
                content_type = "geo"
            elif isinstance(msg.media, MessageMediaGeoLive):
                content_type = "geo_live"
            elif isinstance(msg.media, MessageMediaContact):
                content_type = "contact"
            elif isinstance(msg.media, MessageMediaPoll):
                content_type = "poll"
            elif isinstance(msg.media, MessageMediaDice):
                content_type = "dice"
            elif isinstance(msg.media, MessageMediaInvoice):
                content_type = "invoice"
            elif isinstance(msg.media, MessageMediaGame):
                content_type = "game"
            elif isinstance(msg.media, MessageMediaStory):
                content_type = "story"
            else:
                content_type = "other"

        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = f"""
                    INSERT IGNORE INTO `{MESSAGE_TABLE}` (tg_account_id, chat_id, message_id, sender_user_id,
                        sender_chat_id, sender_name, is_outgoing, send_time, content_type, text_content,
                        media_file_id, media_file_size, media_mime_type, media_file_name, media_duration,
                        media_width, media_height, thumbnail_file_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cur.execute(sql, (account_id, chat_id, message_id, sender_user_id,
                                        sender_chat_id, sender_name, is_outgoing, send_time,
                                        content_type, text_content, media_file_id, media_file_size,
                                        media_mime_type, media_file_name, media_duration,
                                        media_width, media_height, thumbnail_file_id))
    except Exception as e:
        logger.error(f"Failed to insert message {msg.id}: {e}")
