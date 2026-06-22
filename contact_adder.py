"""Contact adder for tg-gc-server (cluster node).

Polls every 15s for pending contacts assigned to this node.
Supports two add methods:
  - one_by_one: AddContactRequest per contact (individual add)
  - contact_import: ImportContactsRequest batch upload (address book style)
Concurrent execution with error isolation per contact.
"""
import asyncio
import logging
from collections import defaultdict

from telethon.tl.functions.contacts import ImportContactsRequest, AddContactRequest
from telethon.tl.types import InputPhoneContact

import config
import database
import node_manager
import client_manager

logger = logging.getLogger(__name__)

CONCURRENCY_LIMIT = 5


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
    """Query pending contacts for this node and process them."""
    node_id = node_manager.NODE_ID
    pending = await database.get_pending_contacts_by_node(node_id)

    if not pending:
        return

    # Group by (account_id, add_method)
    by_account_method: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for item in pending:
        account_id = item.get("tg_account_id") or item.get("account_id")
        add_method = item.get("add_method") or "one_by_one"
        by_account_method[(account_id, add_method)].append(item)

    # Process each group concurrently
    tasks = []
    for (account_id, add_method), items in by_account_method.items():
        tasks.append(_process_account_group(account_id, add_method, items))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _process_account_group(account_id: int, add_method: str, items: list[dict]):
    """Process a group of contacts for one account with one add method."""
    account = await database.get_account_by_id(account_id)
    if not account:
        logger.warning(f"Account {account_id} not found, marking items as failed")
        for item in items:
            await database.update_contact_assign_status(
                item["id"], "failed", error_reason="账号不存在"
            )
        return

    phone = account["phone"]
    tg_client = client_manager.active_clients.get(phone)
    if not tg_client:
        logger.warning(f"[{phone}] Account not online, skipping contact add")
        return

    if add_method == "contact_import":
        await _batch_import_contacts(tg_client, phone, account_id, items)
    else:
        await _add_contacts_one_by_one(tg_client, phone, account_id, items)


async def _add_contacts_one_by_one(tg_client, phone: str, account_id: int, items: list[dict]):
    """Add contacts one by one using AddContactRequest, concurrently with error isolation."""
    node_id = node_manager.NODE_ID
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def add_single(item):
        async with semaphore:
            contact_phone = item.get("contact_phone", "")
            contact_username = item.get("contact_username", "")
            try:
                if contact_phone:
                    result = await tg_client(AddContactRequest(
                        id=contact_phone,
                        first_name=contact_phone,
                        last_name="",
                        phone=contact_phone,
                        add_phone_privacy_exception=True,
                    ))
                    if result and result.users:
                        user = result.users[0]
                        await database.upsert_contact(
                            tg_account_id=account_id,
                            user_id=user.id,
                            first_name=getattr(user, "first_name", None),
                            last_name=getattr(user, "last_name", None),
                            nickname=getattr(user, "first_name", ""),
                            username=getattr(user, "username", None),
                            phone_number=contact_phone,
                            source="import",
                            node_id=node_id,
                        )
                        await database.update_contact_assign_status(
                            item["id"], "success", result_user_id=user.id
                        )
                        logger.info(f"[{phone}] Added contact by phone: {contact_phone} -> {user.id}")
                    else:
                        await database.update_contact_assign_status(
                            item["id"], "failed", error_reason="号码未注册TG"
                        )
                        logger.warning(f"[{phone}] AddContact no result for {contact_phone}")
                elif contact_username:
                    entity = await tg_client.get_entity(contact_username)
                    if entity:
                        result = await tg_client(AddContactRequest(
                            id=entity,
                            first_name=getattr(entity, "first_name", "") or contact_username,
                            last_name=getattr(entity, "last_name", "") or "",
                            phone="",
                            add_phone_privacy_exception=True,
                        ))
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
                    else:
                        await database.update_contact_assign_status(
                            item["id"], "failed", error_reason="用户不存在"
                        )
                else:
                    await database.update_contact_assign_status(
                        item["id"], "failed", error_reason="无手机号和用户名"
                    )
            except Exception as e:
                logger.warning(f"[{phone}] Failed to add contact {contact_phone or contact_username}: {e}")
                await database.update_contact_assign_status(
                    item["id"], "failed", error_reason=str(e)[:500]
                )

    tasks = [add_single(item) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    success_count = sum(1 for r in results if r is None)
    fail_count = len(results) - success_count
    logger.info(f"[{phone}] one_by_one: {success_count}/{len(items)} success, {fail_count} failed")


async def _batch_import_contacts(tg_client, phone: str, account_id: int, items: list[dict]):
    """Import contacts via Telethon's ImportContactsRequest (upload address book style).
    Username-based contacts are handled individually with concurrency.
    Phone-based contacts are batched into one ImportContactsRequest."""
    node_id = node_manager.NODE_ID

    phone_items = []
    username_items = []
    for item in items:
        if item.get("contact_phone"):
            phone_items.append(item)
        elif item.get("contact_username"):
            username_items.append(item)
        else:
            await database.update_contact_assign_status(
                item["id"], "failed", error_reason="无手机号和用户名"
            )

    # Handle username-based contacts concurrently (can't batch these)
    if username_items:
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def add_by_username(item):
            async with semaphore:
                contact_username = item["contact_username"]
                try:
                    entity = await tg_client.get_entity(contact_username)
                    if entity:
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
                        logger.info(f"[{phone}] Import contact by username: {contact_username} -> {entity.id}")
                    else:
                        await database.update_contact_assign_status(
                            item["id"], "failed", error_reason="用户不存在"
                        )
                except Exception as e:
                    await database.update_contact_assign_status(
                        item["id"], "failed", error_reason=str(e)[:500]
                    )
                    logger.warning(f"[{phone}] Failed import by username {contact_username}: {e}")

        username_tasks = [add_by_username(item) for item in username_items]
        await asyncio.gather(*username_tasks, return_exceptions=True)

    # Handle phone-based contacts via ImportContactsRequest
    if not phone_items:
        return

    contacts = []
    item_map = {}
    for i, item in enumerate(phone_items):
        contact_phone = item["contact_phone"]
        contact = InputPhoneContact(
            client_id=i,
            phone=contact_phone,
            first_name=contact_phone,
            last_name="",
        )
        contacts.append(contact)
        item_map[i] = item

    try:
        result = await tg_client(ImportContactsRequest(contacts))

        imported_users = {u.id: u for u in (result.users or [])}
        retry_contacts = set(result.retry_contacts or [])

        for user in (result.imported or []):
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
            f"[{phone}] contact_import: {len(result.imported or [])}/{len(contacts)} success, "
            f"{len(retry_contacts)} retry"
        )
    except Exception as e:
        logger.error(f"[{phone}] ImportContactsRequest failed: {e}")
        for i, item in item_map.items():
            await database.update_contact_assign_status(
                item["id"], "failed", error_reason=str(e)[:500]
            )
