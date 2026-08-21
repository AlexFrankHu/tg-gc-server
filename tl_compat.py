"""Compatibility layer for Telegram servers that answer with an outdated TL layer.

Production symptom (2026-08-21):

    TypeNotFoundError: Could not find a matching Constructor ID for the TLObject
    that was supposed to be read with ID 8f97c628

`user#8f97c628` is the `user` constructor of layer 145~158, `dialog#d58a08c6` the
`dialog` constructor of layer 158~224. Telethon 1.44.0 speaks layer 227 and cannot
read them, so every response containing one raises and the whole request fails
(resolvePhone -> fake contact task failed, get_dialogs -> no data collected,
get_me -> updates catch-up after reconnect aborted).

Root cause: `MTProtoSender._reconnect()` resets the MTProto session id but never
re-sends `invokeWithLayer(LAYER, initConnection(...))`, which Telethon only sends
once in `_connect()`. Without it the server falls back to the layer it last knew
for the auth key (these sessions were created years ago on layer ~148), so every
request issued on a reconnected connection is answered with ancient constructors.

Two independent fixes, both installed by `install()`:

1. `patch_reconnect_layer()` re-negotiates the layer right after every automatic
   reconnect, before any other request is sent. This removes the cause.
2. `register_legacy_tlobjects()` teaches Telethon to read the legacy constructors,
   so responses already in flight (and anything answered at an old layer for
   reasons outside our control) are parsed instead of raising.
"""
import logging

from telethon import functions
from telethon.client import telegrambaseclient, updates as updates_module
from telethon.tl import alltlobjects
from telethon.tl.tlobject import TLObject
from telethon.tl.types import Dialog, User

logger = logging.getLogger(__name__)

_installed = False


class LegacyUser(TLObject):
    """`user#8f97c628` (layer 145~158). Deserialises into a modern `User`."""

    CONSTRUCTOR_ID = 0x8F97C628
    SUBCLASS_OF_ID = User.SUBCLASS_OF_ID

    @classmethod
    def from_reader(cls, reader):
        flags = reader.read_int()
        flags2 = reader.read_int()
        user_id = reader.read_long()
        access_hash = reader.read_long() if flags & 1 else None
        first_name = reader.tgread_string() if flags & 2 else None
        last_name = reader.tgread_string() if flags & 4 else None
        username = reader.tgread_string() if flags & 8 else None
        phone = reader.tgread_string() if flags & 16 else None
        photo = reader.tgread_object() if flags & 32 else None
        status = reader.tgread_object() if flags & 64 else None
        bot_info_version = reader.read_int() if flags & 16384 else None
        restriction_reason = _read_vector(reader) if flags & 262144 else None
        bot_inline_placeholder = reader.tgread_string() if flags & 524288 else None
        lang_code = reader.tgread_string() if flags & 4194304 else None
        emoji_status = reader.tgread_object() if flags & 1073741824 else None
        usernames = _read_vector(reader) if flags2 & 1 else None
        return User(
            id=user_id,
            is_self=bool(flags & 1024),
            contact=bool(flags & 2048),
            mutual_contact=bool(flags & 4096),
            deleted=bool(flags & 8192),
            bot=bool(flags & 16384),
            bot_chat_history=bool(flags & 32768),
            bot_nochats=bool(flags & 65536),
            verified=bool(flags & 131072),
            restricted=bool(flags & 262144),
            min=bool(flags & 1048576),
            bot_inline_geo=bool(flags & 2097152),
            support=bool(flags & 8388608),
            scam=bool(flags & 16777216),
            apply_min_photo=bool(flags & 33554432),
            fake=bool(flags & 67108864),
            bot_attach_menu=bool(flags & 134217728),
            premium=bool(flags & 268435456),
            attach_menu_enabled=bool(flags & 536870912),
            bot_can_edit=bool(flags2 & 2),
            access_hash=access_hash,
            first_name=first_name,
            last_name=last_name,
            username=username,
            phone=phone,
            photo=photo,
            status=status,
            bot_info_version=bot_info_version,
            restriction_reason=restriction_reason,
            bot_inline_placeholder=bot_inline_placeholder,
            lang_code=lang_code,
            emoji_status=emoji_status,
            usernames=usernames,
        )


class LegacyUser214(TLObject):
    """`user#020b1422` (layer 199~216). Deserialises into a modern `User`."""

    CONSTRUCTOR_ID = 0x020B1422
    SUBCLASS_OF_ID = User.SUBCLASS_OF_ID

    @classmethod
    def from_reader(cls, reader):
        flags = reader.read_int()
        flags2 = reader.read_int()
        user_id = reader.read_long()
        access_hash = reader.read_long() if flags & 1 else None
        first_name = reader.tgread_string() if flags & 2 else None
        last_name = reader.tgread_string() if flags & 4 else None
        username = reader.tgread_string() if flags & 8 else None
        phone = reader.tgread_string() if flags & 16 else None
        photo = reader.tgread_object() if flags & 32 else None
        status = reader.tgread_object() if flags & 64 else None
        bot_info_version = reader.read_int() if flags & 16384 else None
        restriction_reason = _read_vector(reader) if flags & 262144 else None
        bot_inline_placeholder = reader.tgread_string() if flags & 524288 else None
        lang_code = reader.tgread_string() if flags & 4194304 else None
        emoji_status = reader.tgread_object() if flags & 1073741824 else None
        usernames = _read_vector(reader) if flags2 & 1 else None
        stories_max_id = reader.read_int() if flags2 & 32 else None
        color = reader.tgread_object() if flags2 & 256 else None
        profile_color = reader.tgread_object() if flags2 & 512 else None
        bot_active_users = reader.read_int() if flags2 & 4096 else None
        bot_verification_icon = reader.read_long() if flags2 & 16384 else None
        send_paid_messages_stars = reader.read_long() if flags2 & 32768 else None
        return User(
            id=user_id,
            is_self=bool(flags & 1024),
            contact=bool(flags & 2048),
            mutual_contact=bool(flags & 4096),
            deleted=bool(flags & 8192),
            bot=bool(flags & 16384),
            bot_chat_history=bool(flags & 32768),
            bot_nochats=bool(flags & 65536),
            verified=bool(flags & 131072),
            restricted=bool(flags & 262144),
            min=bool(flags & 1048576),
            bot_inline_geo=bool(flags & 2097152),
            support=bool(flags & 8388608),
            scam=bool(flags & 16777216),
            apply_min_photo=bool(flags & 33554432),
            fake=bool(flags & 67108864),
            bot_attach_menu=bool(flags & 134217728),
            premium=bool(flags & 268435456),
            attach_menu_enabled=bool(flags & 536870912),
            bot_can_edit=bool(flags2 & 2),
            close_friend=bool(flags2 & 4),
            stories_hidden=bool(flags2 & 8),
            stories_unavailable=bool(flags2 & 16),
            contact_require_premium=bool(flags2 & 1024),
            bot_business=bool(flags2 & 2048),
            bot_has_main_app=bool(flags2 & 8192),
            access_hash=access_hash,
            first_name=first_name,
            last_name=last_name,
            username=username,
            phone=phone,
            photo=photo,
            status=status,
            bot_info_version=bot_info_version,
            restriction_reason=restriction_reason,
            bot_inline_placeholder=bot_inline_placeholder,
            lang_code=lang_code,
            emoji_status=emoji_status,
            usernames=usernames,
            stories_max_id=stories_max_id,
            color=color,
            profile_color=profile_color,
            bot_active_users=bot_active_users,
            bot_verification_icon=bot_verification_icon,
            send_paid_messages_stars=send_paid_messages_stars,
        )


class LegacyDialog(TLObject):
    """`dialog#d58a08c6` (layer 158~224). Deserialises into a modern `Dialog`."""

    CONSTRUCTOR_ID = 0xD58A08C6
    SUBCLASS_OF_ID = Dialog.SUBCLASS_OF_ID

    @classmethod
    def from_reader(cls, reader):
        flags = reader.read_int()
        peer = reader.tgread_object()
        top_message = reader.read_int()
        read_inbox_max_id = reader.read_int()
        read_outbox_max_id = reader.read_int()
        unread_count = reader.read_int()
        unread_mentions_count = reader.read_int()
        unread_reactions_count = reader.read_int()
        notify_settings = reader.tgread_object()
        pts = reader.read_int() if flags & 1 else None
        draft = reader.tgread_object() if flags & 2 else None
        folder_id = reader.read_int() if flags & 16 else None
        ttl_period = reader.read_int() if flags & 32 else None
        return Dialog(
            peer=peer,
            top_message=top_message,
            read_inbox_max_id=read_inbox_max_id,
            read_outbox_max_id=read_outbox_max_id,
            unread_count=unread_count,
            unread_mentions_count=unread_mentions_count,
            unread_reactions_count=unread_reactions_count,
            unread_poll_votes_count=0,
            notify_settings=notify_settings,
            pinned=bool(flags & 4),
            unread_mark=bool(flags & 8),
            pts=pts,
            draft=draft,
            folder_id=folder_id,
            ttl_period=ttl_period,
        )


class LegacyDialogA8(LegacyDialog):
    """`dialog#a8edd0f5` (layer <=157): same as `dialog#d58a08c6` without ttl_period."""

    CONSTRUCTOR_ID = 0xA8EDD0F5

    @classmethod
    def from_reader(cls, reader):
        flags = reader.read_int()
        peer = reader.tgread_object()
        top_message = reader.read_int()
        read_inbox_max_id = reader.read_int()
        read_outbox_max_id = reader.read_int()
        unread_count = reader.read_int()
        unread_mentions_count = reader.read_int()
        unread_reactions_count = reader.read_int()
        notify_settings = reader.tgread_object()
        pts = reader.read_int() if flags & 1 else None
        draft = reader.tgread_object() if flags & 2 else None
        folder_id = reader.read_int() if flags & 16 else None
        return Dialog(
            peer=peer,
            top_message=top_message,
            read_inbox_max_id=read_inbox_max_id,
            read_outbox_max_id=read_outbox_max_id,
            unread_count=unread_count,
            unread_mentions_count=unread_mentions_count,
            unread_reactions_count=unread_reactions_count,
            unread_poll_votes_count=0,
            notify_settings=notify_settings,
            pinned=bool(flags & 4),
            unread_mark=bool(flags & 8),
            pts=pts,
            draft=draft,
            folder_id=folder_id,
        )


LEGACY_TLOBJECTS = (LegacyUser, LegacyUser214, LegacyDialog, LegacyDialogA8)


def _read_vector(reader):
    reader.read_int()  # vector#1cb5c415
    return [reader.tgread_object() for _ in range(reader.read_int())]


def register_legacy_tlobjects() -> int:
    """Register legacy constructors so Telethon can read old-layer responses."""
    added = 0
    for obj in LEGACY_TLOBJECTS:
        if alltlobjects.tlobjects.get(obj.CONSTRUCTOR_ID) is None:
            alltlobjects.tlobjects[obj.CONSTRUCTOR_ID] = obj
            added += 1
    logger.info("tl_compat: registered %d legacy constructor(s)", added)
    return added


def patch_reconnect_layer() -> None:
    """Re-send invokeWithLayer(initConnection) after every automatic reconnect."""
    original = updates_module.UpdateMethods._handle_auto_reconnect

    async def _handle_auto_reconnect(self):
        try:
            init = self._init_request
            init.query = functions.help.GetConfigRequest()
            request = functions.InvokeWithoutUpdatesRequest(init) if self._no_updates else init
            await self._sender.send(
                functions.InvokeWithLayerRequest(telegrambaseclient.LAYER, request)
            )
        except Exception as e:
            logger.warning("tl_compat: re-negotiate layer after reconnect failed: %s", e)
        await original(self)

    updates_module.UpdateMethods._handle_auto_reconnect = _handle_auto_reconnect


def install() -> None:
    """Idempotently install both patches. Call once before creating any client."""
    global _installed
    if _installed:
        return
    register_legacy_tlobjects()
    patch_reconnect_layer()
    _installed = True
