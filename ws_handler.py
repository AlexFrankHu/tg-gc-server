"""WebSocket handler for the web client."""
import json
import logging
import asyncio
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect
from telethon.tl.types import (
    User, Chat, Channel, Message,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage,
    Document, Photo, PeerUser, PeerChat, PeerChannel,
    DocumentAttributeFilename, DocumentAttributeVideo,
    DocumentAttributeAudio, DocumentAttributeSticker,
)
from telethon import events

import client_manager

logger = logging.getLogger(__name__)

# Active WebSocket connections: ws -> {account_id, phone}
active_connections: dict[WebSocket, dict] = {}


async def websocket_endpoint(websocket: WebSocket, token: str = None):
    """Handle a WebSocket connection from the web client."""
    # Validate token
    import auth
    payload = auth.validate_token(token)
    if not payload:
        await websocket.accept()
        await websocket.close(code=4001, reason="Invalid or expired token")
        return

    await websocket.accept()
    account_id = payload.get("account_id")
    phone = payload.get("phone")
    connection_info = {"account_id": account_id, "phone": phone}
    active_connections[websocket] = connection_info
    event_handler = None

    # Auto-register event handler if account is online
    if phone and phone in client_manager.active_clients:
        client = client_manager.active_clients[phone]
        event_handler = _create_event_handler(websocket, account_id)
        client.add_event_handler(event_handler, events.NewMessage)

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")
            request_id = msg.get("requestId")
            msg_data = msg.get("data", {})

            if msg_type == "ping":
                await send_ws(websocket, "pong", {}, request_id)
            elif msg_type == "tg.account.switch":
                # Re-register event handler if switching
                req_account_id = msg_data.get("tgAccountId")
                req_phone = await _get_phone_by_id(req_account_id)
                if req_phone and req_phone in client_manager.active_clients:
                    # Remove old handler if different account
                    if event_handler and connection_info["phone"] and connection_info["phone"] != req_phone:
                        old_phone = connection_info["phone"]
                        if old_phone in client_manager.active_clients:
                            client_manager.active_clients[old_phone].remove_event_handler(event_handler, events.NewMessage)
                    connection_info["account_id"] = req_account_id
                    connection_info["phone"] = req_phone
                    client = client_manager.active_clients[req_phone]
                    event_handler = _create_event_handler(websocket, req_account_id)
                    client.add_event_handler(event_handler, events.NewMessage)
                    await send_ws(websocket, "tg.account.switch.ok", {"tgAccountId": req_account_id}, request_id)
                else:
                    await send_ws(websocket, "system.error", {"message": "Account not online"}, request_id)
            elif msg_type == "tg.chat.list":
                await handle_chat_list(websocket, connection_info, msg_data, request_id)
            elif msg_type == "tg.chat.history":
                await handle_chat_history(websocket, connection_info, msg_data, request_id)
            elif msg_type == "tg.message.send":
                await handle_send_message(websocket, connection_info, msg_data, request_id)
            elif msg_type == "tg.chat.members":
                await handle_chat_members(websocket, connection_info, msg_data, request_id)
            elif msg_type == "tg.message.mark-read":
                await handle_mark_read(websocket, connection_info, msg_data, request_id)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Remove event handler
        if event_handler and connection_info["phone"]:
            phone = connection_info["phone"]
            if phone in client_manager.active_clients:
                client = client_manager.active_clients[phone]
                client.remove_event_handler(event_handler, events.NewMessage)
        active_connections.pop(websocket, None)


async def send_ws(websocket: WebSocket, msg_type: str, data: dict, request_id: str = None):
    """Send a message through the WebSocket."""
    msg = {"type": msg_type, "data": data}
    if request_id:
        msg["requestId"] = request_id
    try:
        await websocket.send_text(json.dumps(msg, default=str))
    except Exception:
        pass


async def _get_phone_by_id(account_id: int) -> str:
    """Get phone number by account ID from database."""
    import database
    accounts = await database.get_all_accounts()
    for acc in accounts:
        if acc["id"] == account_id:
            return acc["phone"]
    return None


def _create_event_handler(websocket: WebSocket, account_id: int):
    """Create an event handler for new messages that sends to the WebSocket."""
    async def handler(event):
        try:
            msg = event.message
            sender = await event.get_sender()
            sender_name = ""
            if isinstance(sender, User):
                sender_name = " ".join(filter(None, [sender.first_name, sender.last_name]))

            chat_id = event.chat_id
            data = {
                "tgAccountId": account_id,
                "chatId": chat_id,
                "messageId": msg.id,
                "senderUserId": sender.id if sender else None,
                "senderName": sender_name,
                "outgoing": msg.out,
                "date": int(msg.date.timestamp()) if msg.date else None,
                "contentType": _get_content_type(msg),
                "textPreview": msg.text[:200] if msg.text else "",
                "media": _get_media_info(msg)
            }
            await send_ws(websocket, "tg.message.new", data)
        except Exception as e:
            logger.error(f"Event handler error: {e}")
    return handler


async def handle_chat_list(websocket: WebSocket, conn_info: dict, data: dict, request_id: str):
    """Handle chat list request."""
    phone = conn_info.get("phone")
    if not phone or phone not in client_manager.active_clients:
        await send_ws(websocket, "system.error", {"message": "Not connected"}, request_id)
        return

    client = client_manager.active_clients[phone]
    limit = data.get("limit", 100)

    try:
        dialogs = await client.get_dialogs(limit=limit)
        chats = []
        for dialog in dialogs:
            chat_type = "private"
            if dialog.is_group:
                chat_type = "basic_group"
            elif dialog.is_channel:
                entity = dialog.entity
                if isinstance(entity, Channel) and entity.megagroup:
                    chat_type = "supergroup"
                else:
                    chat_type = "channel"

            last_msg = dialog.message
            last_preview = ""
            last_date = None
            if last_msg:
                last_preview = last_msg.text[:100] if last_msg.text else ""
                last_date = int(last_msg.date.timestamp()) if last_msg.date else None

            chats.append({
                "chatId": dialog.entity.id,
                "title": dialog.title or dialog.name or "Unknown",
                "type": chat_type,
                "unreadCount": dialog.unread_count,
                "lastMessageDate": last_date,
                "lastMessagePreview": last_preview,
            })

        await send_ws(websocket, "tg.chat.list", {"chats": chats}, request_id)
    except Exception as e:
        logger.error(f"Error getting chat list: {e}")
        await send_ws(websocket, "system.error", {"message": str(e)}, request_id)


async def handle_chat_history(websocket: WebSocket, conn_info: dict, data: dict, request_id: str):
    """Handle chat history request."""
    phone = conn_info.get("phone")
    if not phone or phone not in client_manager.active_clients:
        await send_ws(websocket, "system.error", {"message": "Not connected"}, request_id)
        return

    client = client_manager.active_clients[phone]
    chat_id = data.get("chatId")
    from_message_id = data.get("fromMessageId", 0)
    limit = data.get("limit", 50)

    try:
        messages = await client.get_messages(
            chat_id,
            limit=limit,
            offset_id=from_message_id if from_message_id else 0
        )

        result_messages = []
        for msg in messages:
            if not isinstance(msg, Message):
                continue
            sender_name = ""
            if msg.sender:
                if isinstance(msg.sender, User):
                    sender_name = " ".join(filter(None, [msg.sender.first_name, msg.sender.last_name]))
                elif isinstance(msg.sender, (Chat, Channel)):
                    sender_name = msg.sender.title or ""

            result_messages.append({
                "messageId": msg.id,
                "chatId": chat_id,
                "senderUserId": msg.sender_id.user_id if msg.sender_id and hasattr(msg.sender_id, "user_id") else None,
                "senderName": sender_name,
                "outgoing": msg.out,
                "date": int(msg.date.timestamp()) if msg.date else None,
                "contentType": _get_content_type(msg),
                "textPreview": msg.text[:2000] if msg.text else "",
                "media": _get_media_info(msg)
            })

        await send_ws(websocket, "tg.chat.history", {
            "chatId": chat_id,
            "messages": result_messages
        }, request_id)
    except Exception as e:
        logger.error(f"Error getting history: {e}")
        await send_ws(websocket, "system.error", {"message": str(e)}, request_id)


async def handle_send_message(websocket: WebSocket, conn_info: dict, data: dict, request_id: str):
    """Handle send message request."""
    phone = conn_info.get("phone")
    if not phone or phone not in client_manager.active_clients:
        await send_ws(websocket, "system.error", {"message": "Not connected"}, request_id)
        return

    client = client_manager.active_clients[phone]
    chat_id = data.get("chatId")
    text = data.get("text", "")

    try:
        msg = await client.send_message(chat_id, text)
        sender_name = ""
        me = await client.get_me()
        if me:
            sender_name = " ".join(filter(None, [me.first_name, me.last_name]))

        result = {
            "chatId": chat_id,
            "message": {
                "messageId": msg.id,
                "chatId": chat_id,
                "senderUserId": me.id if me else None,
                "senderName": sender_name,
                "outgoing": True,
                "date": int(msg.date.timestamp()) if msg.date else None,
                "contentType": "text",
                "textPreview": text[:2000],
                "media": None
            }
        }
        await send_ws(websocket, "tg.message.send.result", result, request_id)
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        await send_ws(websocket, "system.error", {"message": str(e)}, request_id)


async def handle_chat_members(websocket: WebSocket, conn_info: dict, data: dict, request_id: str):
    """Handle chat members request for groups."""
    phone = conn_info.get("phone")
    if not phone or phone not in client_manager.active_clients:
        await send_ws(websocket, "system.error", {"message": "Not connected"}, request_id)
        return

    client = client_manager.active_clients[phone]
    chat_id = data.get("chatId")
    limit = data.get("limit", 200)

    try:
        participants = await client.get_participants(chat_id, limit=limit)
        members = []
        for p in participants:
            if isinstance(p, User):
                members.append({
                    "userId": p.id,
                    "firstName": p.first_name or "",
                    "lastName": p.last_name or "",
                    "username": p.username,
                })
        await send_ws(websocket, "tg.chat.members", {"members": members}, request_id)
    except Exception as e:
        logger.error(f"Error getting members: {e}")
        await send_ws(websocket, "tg.chat.members", {"members": []}, request_id)


async def handle_mark_read(websocket: WebSocket, conn_info: dict, data: dict, request_id: str):
    """Handle mark as read request."""
    phone = conn_info.get("phone")
    if not phone or phone not in client_manager.active_clients:
        return

    client = client_manager.active_clients[phone]
    chat_id = data.get("chatId")

    try:
        await client.send_read_acknowledge(chat_id)
    except Exception as e:
        logger.debug(f"Mark read error: {e}")


def _get_content_type(msg) -> str:
    """Determine message content type."""
    if not msg.media:
        return "text"
    if isinstance(msg.media, MessageMediaPhoto):
        return "photo"
    if isinstance(msg.media, MessageMediaDocument):
        doc = msg.media.document
        if isinstance(doc, Document):
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeSticker):
                    return "sticker"
                if isinstance(attr, DocumentAttributeVideo):
                    return "video"
                if isinstance(attr, DocumentAttributeAudio):
                    return "voice" if attr.voice else "audio"
            return "document"
    if isinstance(msg.media, MessageMediaWebPage):
        return "text"
    return "other"


def _get_media_info(msg) -> dict:
    """Extract media info from a message."""
    if not msg.media:
        return None

    if isinstance(msg.media, MessageMediaPhoto):
        photo = msg.media.photo
        if isinstance(photo, Photo):
            return {
                "kind": "photo",
                "fileId": str(photo.id),
            }
    elif isinstance(msg.media, MessageMediaDocument):
        doc = msg.media.document
        if isinstance(doc, Document):
            info = {
                "kind": "document",
                "fileId": str(doc.id),
                "mimeType": doc.mime_type,
                "size": doc.size,
            }
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    info["fileName"] = attr.file_name
                elif isinstance(attr, DocumentAttributeSticker):
                    info["kind"] = "sticker"
                    info["stickerEmoji"] = attr.alt
                elif isinstance(attr, DocumentAttributeVideo):
                    info["kind"] = "video"
                    info["duration"] = attr.duration
                    info["width"] = attr.w
                    info["height"] = attr.h
                elif isinstance(attr, DocumentAttributeAudio):
                    info["kind"] = "voice" if attr.voice else "audio"
                    info["duration"] = attr.duration
            return info
    return None
