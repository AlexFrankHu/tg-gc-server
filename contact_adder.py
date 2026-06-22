"""Contact adder for tg-gc-server (cluster node).

Polls every 15s for pending contacts assigned to this node.
Groups by account and uses ImportContactsRequest for batch upload.
"""
import asyncio
import logging
from collections import defaultdict

from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact

import config
import database
import node_manager
import client_manager

logger = logging.getLogger(__name__)


async def poll_contact_adder():
    """Background loop: process pending contacts every 15 seconds."""
    while True:
        try:
            await asyncio.sleep(config.CONTACT_ADDER_INTERVAL)
            await _process_pending_contacts()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Contact adder poll error: {e}")


async def _process_pending_contacts():
    """Query pending contacts for this node and add them via ImportContactsRequest."""
    node_id = node_manager.NODE_ID
    pending = await database.get_pending_contacts_by_node(node_id)

    if not pending:
        return

    # Group by account
    by_account: dict[int, list[dict]] = defaultdict(list)
    for item in pending:
        by_account[item["tg_account_id"]].append(item)

    for account_id, items in by_account.items():
        account = await database.get_account_by_id(account_id)
        if not account:
            continue

        phone = account["phone"]
        tg_client = client_manager.active_clients.get(phone)
        if not tg_client:
            logger.warning(f"[{phone}] Account not online, skipping contact add")
            continue

        try:
            await _batch_import_contacts(tg_client, phone, account_id, items)
        except Exception as e:
            logger.error(f"[{phone}] Batch import contacts error: {e}")
            for item in items:
                await database.update_contact_assign_status(
                    item["id"], "failed", error_reason=str(e)
                )


async def _batch_import_contacts(tg_client, phone: str, account_id: int, items: list[dict]):
    """Import contacts via Telethon's ImportContactsRequest (upload address book style)."""
    node_id = node_manager.NODE_ID
    contacts = []
    item_map = {}

    for i, item in enumerate(items):
        contact_phone = item.get("contact_phone", "")
        contact_username = item.get("contact_username", "")

        if contact_phone:
            contact = InputPhoneContact(
                client_id=i,
                phone=contact_phone,
                first_name=contact_phone,
                last_name="",
            )
            contacts.append(contact)
            item_map[i] = item
        elif contact_username:
            # For username-based adds, we handle separately
            try:
                entity = await tg_client.get_entity(contact_username)
                if entity:
                    # Save to tg_contact
                    await database.upsert_contact(
                        tg_account_id=account_id,
                        user_id=entity.id,
                        first_name=getattr(entity, "first_name", None),
                        last_name=getattr(entity, "last_name", None),
                        nickname=getattr(entity, "first_name", ""),
                        username=getattr(entity, "username", None),
                        source="import",
                        node_id=node_id,
                    )
                    await database.update_contact_assign_status(
                        item["id"], "success", result_user_id=entity.id
                    )
                    logger.info(f"[{phone}] Added contact by username: {contact_username} -> {entity.id}")
            except Exception as e:
                await database.update_contact_assign_status(
                    item["id"], "failed", error_reason=str(e)
                )
                logger.warning(f"[{phone}] Failed to add by username {contact_username}: {e}")
            continue

    if not contacts:
        return

    try:
        result = await tg_client(ImportContactsRequest(contacts))

        imported_users = {u.id: u for u in (result.users or [])}
        retry_contacts = set(result.retry_contacts or [])

        for i, user in enumerate(result.imported or []):
            item = item_map.get(user.client_id)
            if not item:
                continue
            tg_user = imported_users.get(user.user_id)
            if tg_user:
                await database.upsert_contact(
                    tg_account_id=account_id,
                    user_id=user.user_id,
                    first_name=getattr(tg_user, "first_name", None),
                    last_name=getattr(tg_user, "last_name", None),
                    nickname=getattr(tg_user, "first_name", ""),
                    username=getattr(tg_user, "username", None),
                    phone_number=item.get("contact_phone"),
                    source="import",
                    node_id=node_id,
                )
            await database.update_contact_assign_status(
                item["id"], "success", result_user_id=user.user_id
            )

        # Mark non-imported items
        imported_client_ids = {u.client_id for u in (result.imported or [])}
        for i, item in item_map.items():
            if i not in imported_client_ids:
                if i in retry_contacts:
                    await database.update_contact_assign_status(
                        item["id"], "pending", error_reason="需要重试"
                    )
                else:
                    await database.update_contact_assign_status(
                        item["id"], "failed", error_reason="号码未注册TG"
                    )

        logger.info(
            f"[{phone}] ImportContacts: {len(result.imported or [])}/{len(contacts)} success, "
            f"{len(retry_contacts)} retry"
        )
    except Exception as e:
        logger.error(f"[{phone}] ImportContactsRequest failed: {e}")
        for i, item in item_map.items():
            await database.update_contact_assign_status(
                item["id"], "failed", error_reason=str(e)
            )
