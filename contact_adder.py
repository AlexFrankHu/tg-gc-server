"""Contact adder for tg-gc-server (cluster node).

Polls every 15s for pending contacts assigned to this node.
Two contact types:
  - real: add to the account's TG contact list (importContacts / addContact)
  - fake: only resolve the phone/username to a TG user and store it locally,
          no contact is created on Telegram's side
Real contacts support two add methods:
  - one_by_one: AddContactRequest per contact (individual add)
  - contact_import: ImportContactsRequest batch upload (address book style)
Concurrent execution with error isolation per contact.
"""
import asyncio
import logging
import random
from collections import defaultdict

from telethon.tl.functions.contacts import (
    ImportContactsRequest, AddContactRequest, GetContactsRequest, ResolvePhoneRequest,
)
from telethon.tl.types import InputPhoneContact, InputUser

import config
import database
import node_manager
import client_manager
import tg_errors
import watchdog

logger = logging.getLogger(__name__)

CONCURRENCY_LIMIT = 5
# A single Telegram request must answer within this many seconds; a client stuck in
# an endless reconnect otherwise blocks the whole poll round for every other account.
REQUEST_TIMEOUT = 60
# Upper bound for processing one account's pending items in a poll round.
ACCOUNT_GROUP_TIMEOUT = 300
MAX_RETRY_COUNT = 2  # After 2 retries, mark account as restricted
# Consecutive "phone not occupied" results after which the account itself is
# considered unable to resolve phones anymore and is marked restricted.
NOT_OCCUPIED_RESTRICT_LIMIT = 3

# account_id -> consecutive "phone not occupied" results, cleared once a phone resolves.
_not_occupied_streak: dict[int, int] = {}

# Error meaning the account's connection dropped -> remove from online list and set offline.
DISCONNECTED_ERR = 'Cannot send requests while disconnected'


async def _react_add_failure(err: str, phone: str, account_id: int | None = None):
    """React to specific add-friend failures: disconnect, TG freeze, restriction."""
    if DISCONNECTED_ERR in err:
        await client_manager.handle_disconnected_account(phone)
    if account_id is None:
        return
    if tg_errors.is_frozen_error(err):
        await database.mark_account_frozen(account_id, phone)
    elif tg_errors.is_restrict_error(err):
        await database.mark_account_restricted(account_id, phone)
        logger.warning(f"[{phone}] 加好友返回 PEER_ID_INVALID, 已标记账号受限")
    elif tg_errors.is_not_occupied_error(err):
        await _restrict_if_never_resolves(phone, account_id)


def _note_phone_resolved(account_id: int):
    """A phone resolved for this account, so it is not blind: clear the streak."""
    _not_occupied_streak.pop(account_id, None)


async def _restrict_if_never_resolves(phone: str, account_id: int):
    """Restrict the account after NOT_OCCUPIED_RESTRICT_LIMIT consecutive resolves that
    all report no Telegram user: Telegram stopped resolving phones for this account.
    """
    streak = _not_occupied_streak.get(account_id, 0) + 1
    _not_occupied_streak[account_id] = streak
    if streak < NOT_OCCUPIED_RESTRICT_LIMIT:
        return
    _not_occupied_streak.pop(account_id, None)
    await database.mark_account_restricted(account_id, phone)
    await database.fail_pending_contacts_for_account(
        account_id, node_manager.NODE_ID,
        f"账号连续{NOT_OCCUPIED_RESTRICT_LIMIT}次解析号码均无TG用户，标记为受限",
    )
    logger.warning(
        f"[{phone}] 连续{NOT_OCCUPIED_RESTRICT_LIMIT}次解析号码均返回无TG用户, 已标记账号受限"
    )


def _normalize_phone(phone: str) -> str:
    """Ensure phone number has + prefix."""
    if phone and not phone.startswith("+"):
        return "+" + phone
    return phone


async def poll_contact_adder():
    """Background loop: process pending contacts every 15 seconds."""
    while True:
        try:
            await asyncio.sleep(config.CONTACT_ADDER_INTERVAL)
            await _process_pending_contacts()
            watchdog.ping()
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

    # Group by (account_id, add_method, contact_type)
    by_account_method: dict[tuple[int, str, str], list[dict]] = defaultdict(list)
    for item in pending:
        account_id = item.get("tg_account_id") or item.get("account_id")
        add_method = item.get("add_method") or "one_by_one"
        contact_type = item.get("contact_type") or "real"
        by_account_method[(account_id, add_method, contact_type)].append(item)

    # Process each group concurrently
    tasks = []
    for (account_id, add_method, contact_type), items in by_account_method.items():
        tasks.append(_process_account_group_bounded(account_id, add_method, contact_type, items))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _process_account_group_bounded(account_id: int, add_method: str, contact_type: str,
                                         items: list[dict]):
    try:
        await asyncio.wait_for(
            _process_account_group(account_id, add_method, contact_type, items),
            timeout=ACCOUNT_GROUP_TIMEOUT,
        )
    except asyncio.TimeoutError:
        account = await database.get_account_by_id(account_id)
        phone = account["phone"] if account else str(account_id)
        logger.warning(f"[{phone}] Processing {len(items)} pending contacts exceeded "
                       f"{ACCOUNT_GROUP_TIMEOUT}s, skipping this round")
        tg_client = client_manager.active_clients.get(phone)
        if tg_client and not tg_client.is_connected():
            client_manager.active_clients.pop(phone, None)
            await database.update_account_status(phone, "offline")


async def _process_account_group(account_id: int, add_method: str, contact_type: str,
                                 items: list[dict]):
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

    # Skip restricted or frozen accounts — do not add contacts, mark pending as failed
    if database.is_account_blocked(account):
        logger.info(f"[{phone}] Account is restricted/frozen, marking pending records as failed")
        node_id = node_manager.NODE_ID
        await database.fail_pending_contacts_for_account(
            account_id, node_id, "账号已被限制，无法添加好友"
        )
        return

    tg_client = client_manager.active_clients.get(phone)
    if tg_client and not tg_client.is_connected():
        logger.warning(f"[{phone}] Client in active_clients but disconnected, removing and setting offline")
        client_manager.active_clients.pop(phone, None)
        await database.update_account_status(phone, "offline")
        tg_client = None

    if not tg_client:
        logger.warning(f"[{phone}] Account not online, incrementing retry count")
        node_id = node_manager.NODE_ID
        for item in items:
            retry_count = await database.increment_retry_count(item["id"])
            if retry_count >= MAX_RETRY_COUNT:
                logger.warning(f"[{phone}] Retry count {retry_count} >= {MAX_RETRY_COUNT} (offline), marking account restricted")
                await database.mark_account_restricted(account_id, phone)
                await database.fail_pending_contacts_for_account(
                    account_id, node_id, f"账号离线且重试{retry_count}次仍无法添加好友，标记为受限"
                )
                break
        return

    if contact_type == "fake":
        await _add_fake_contacts(tg_client, phone, account_id, items)
    elif add_method in ("contact_import", "batch_import"):
        await _batch_import_contacts(tg_client, phone, account_id, items)
    else:
        await _add_contacts_one_by_one(tg_client, phone, account_id, items)


async def _add_fake_contacts(tg_client, phone: str, account_id: int, items: list[dict]):
    """Fake contacts: resolve the user and store it, without touching the TG contact list.

    Exactly one Telegram request per item (contacts.resolvePhone for phones,
    contacts.resolveUsername via get_entity for usernames). access_hash is persisted
    because the user is not a contact and cannot be looked up again from getContacts.
    """
    node_id = node_manager.NODE_ID
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def resolve_single(item):
        async with semaphore:
            assign_id = item["id"]
            contact_phone = _normalize_phone(item.get("contact_phone", ""))
            contact_username = (item.get("contact_username") or "").strip().lstrip("@")
            try:
                if contact_phone:
                    resolved = await asyncio.wait_for(
                        tg_client(ResolvePhoneRequest(phone=contact_phone.replace("+", ""))),
                        timeout=REQUEST_TIMEOUT,
                    )
                    user = resolved.users[0] if resolved and resolved.users else None
                elif contact_username:
                    user = await asyncio.wait_for(
                        tg_client.get_entity(contact_username), timeout=REQUEST_TIMEOUT
                    )
                else:
                    await database.update_contact_assign_status(
                        assign_id, "failed", error_reason="无手机号和用户名"
                    )
                    return

                if not user:
                    await database.update_contact_assign_status(
                        assign_id, "failed", error_reason="号码未注册TG或隐私限制(伪好友仅解析)"
                    )
                    await _restrict_if_never_resolves(phone, account_id)
                    return

                await database.upsert_contact(
                    tg_account_id=account_id, user_id=user.id,
                    access_hash=getattr(user, "access_hash", None),
                    first_name=getattr(user, "first_name", None),
                    last_name=getattr(user, "last_name", None),
                    nickname=getattr(user, "first_name", ""),
                    username=getattr(user, "username", None),
                    phone_number=contact_phone or None,
                    is_bot=bool(getattr(user, "bot", False)),
                    is_premium=bool(getattr(user, "premium", False)),
                    source="import", contact_type="fake", node_id=node_id,
                )
                await database.update_contact_assign_status(
                    assign_id, "success", result_user_id=user.id
                )
                _note_phone_resolved(account_id)
                logger.info(f"[{phone}] Fake contact resolved: {contact_phone or contact_username} -> {user.id}")
            except asyncio.TimeoutError:
                logger.warning(f"[{phone}] Resolve fake contact {contact_phone or contact_username} "
                               f"timed out after {REQUEST_TIMEOUT}s")
                await database.update_contact_assign_status(
                    assign_id, "failed", error_reason=f"TG请求超时({REQUEST_TIMEOUT}s)"
                )
            except Exception as e:
                logger.warning(f"[{phone}] Failed to resolve fake contact {contact_phone or contact_username}: {e}")
                await database.update_contact_assign_status(
                    assign_id, "failed", error_reason=str(e)[:500]
                )
                await _react_add_failure(str(e), phone, account_id)

    results = await asyncio.gather(*[resolve_single(item) for item in items],
                                   return_exceptions=True)
    success_count = sum(1 for r in results if r is None)
    logger.info(f"[{phone}] fake: {success_count}/{len(items)} resolved")


async def _add_contacts_one_by_one(tg_client, phone: str, account_id: int, items: list[dict]):
    """Add contacts one by one, concurrently with error isolation.
    For phone: ImportContactsRequest first, fallback to ResolvePhone/AddContactRequest.
    For username: get_entity + AddContactRequest.
    """
    node_id = node_manager.NODE_ID
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def add_single(item):
        async with semaphore:
            contact_phone = _normalize_phone(item.get("contact_phone", ""))
            contact_username = item.get("contact_username", "")
            try:
                if contact_phone:
                    await _add_one_by_phone(tg_client, phone, account_id, node_id, item, contact_phone)
                elif contact_username:
                    await _add_one_by_username(tg_client, phone, account_id, node_id, item, contact_username)
                else:
                    await database.update_contact_assign_status(
                        item["id"], "failed", error_reason="无手机号和用户名"
                    )
            except Exception as e:
                logger.warning(f"[{phone}] Failed to add contact {contact_phone or contact_username}: {e}")
                await database.update_contact_assign_status(
                    item["id"], "failed", error_reason=str(e)[:500]
                )
                await _react_add_failure(str(e), phone, account_id)

    tasks = [add_single(item) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    success_count = sum(1 for r in results if r is None)
    fail_count = len(results) - success_count
    logger.info(f"[{phone}] one_by_one: {success_count}/{len(items)} success, {fail_count} failed")


async def _add_one_by_phone(tg_client, phone: str, account_id: int, node_id: str,
                             item: dict, contact_phone: str):
    """Add a single contact by phone: ImportContactsRequest first, fallback to ResolvePhone."""
    assign_id = item["id"]

    # Step 1: Check if already a contact
    try:
        entity = await tg_client.get_entity(contact_phone)
        if entity:
            result = await tg_client(GetContactsRequest(hash=0))
            phone_clean = contact_phone.replace("+", "")
            for user in result.users:
                if user.phone and user.phone.replace("+", "") == phone_clean:
                    logger.info(f"[{phone}] Already a contact: {contact_phone} -> {user.id}")
                    await database.upsert_contact(
                        tg_account_id=account_id, user_id=user.id,
                        access_hash=getattr(user, "access_hash", None),
                        first_name=getattr(user, "first_name", None),
                        last_name=getattr(user, "last_name", None),
                        nickname=getattr(user, "first_name", ""),
                        username=getattr(user, "username", None),
                        phone_number=contact_phone, source="import", node_id=node_id,
                    )
                    await database.update_contact_assign_status(assign_id, "skipped", result_user_id=user.id)
                    return
    except Exception:
        pass

    # Step 2: ImportContactsRequest (single contact)
    input_contact = InputPhoneContact(
        client_id=random.randint(0, 2**31),
        phone=contact_phone,
        first_name=contact_phone,
        last_name="",
    )
    result = await tg_client(ImportContactsRequest([input_contact]))
    logger.info(f"[{phone}] ImportContacts for {contact_phone}: imported={len(result.imported)}, users={len(result.users)}")

    if result.imported and result.users:
        user = result.users[0]
        await database.upsert_contact(
            tg_account_id=account_id, user_id=user.id,
            access_hash=getattr(user, "access_hash", None),
            first_name=getattr(user, "first_name", None),
            last_name=getattr(user, "last_name", None),
            nickname=getattr(user, "first_name", ""),
            username=getattr(user, "username", None),
            phone_number=contact_phone, source="import", node_id=node_id,
        )
        await database.update_contact_assign_status(assign_id, "success", result_user_id=user.id)
        _note_phone_resolved(account_id)
        logger.info(f"[{phone}] Added contact by phone: {contact_phone} -> {user.id}")
        return

    if result.users:
        user = result.users[0]
        logger.info(f"[{phone}] Already a contact via import: {contact_phone} -> {user.id}")
        await database.upsert_contact(
            tg_account_id=account_id, user_id=user.id,
            access_hash=getattr(user, "access_hash", None),
            first_name=getattr(user, "first_name", None),
            last_name=getattr(user, "last_name", None),
            nickname=getattr(user, "first_name", ""),
            username=getattr(user, "username", None),
            phone_number=contact_phone, source="import", node_id=node_id,
        )
        await database.update_contact_assign_status(assign_id, "skipped", result_user_id=user.id)
        return

    # Step 3: Fallback — ResolvePhoneRequest then AddContactRequest
    user = None
    try:
        phone_clean = contact_phone.replace("+", "")
        resolved = await tg_client(ResolvePhoneRequest(phone=phone_clean))
        if resolved and resolved.users:
            user = resolved.users[0]
            logger.info(f"[{phone}] ResolvePhone found: {contact_phone} -> {user.id}")
    except Exception as e:
        logger.info(f"[{phone}] ResolvePhone failed for {contact_phone}: {e}")

    if not user:
        try:
            entity = await tg_client.get_entity(contact_phone)
            if entity:
                user = entity
                logger.info(f"[{phone}] get_entity found: {contact_phone} -> {user.id}")
        except Exception as e:
            logger.info(f"[{phone}] get_entity failed for {contact_phone}: {e}")

    if not user:
        if result.retry_contacts:
            retry_count = await database.increment_retry_count(assign_id)
            if retry_count >= MAX_RETRY_COUNT:
                await database.update_contact_assign_status(
                    assign_id, "failed", error_reason=f"添加好友被限流,重试{retry_count}次后标记账号受限"
                )
                await database.mark_account_restricted(account_id, phone)
                await database.fail_pending_contacts_for_account(
                    account_id, node_id, f"账号被限制(添加好友连续{retry_count}次被限流)"
                )
                logger.warning(f"[{phone}] Account restricted: import rate-limited {retry_count} times")
            else:
                await database.update_contact_assign_status(
                    assign_id, "pending", error_reason=f"被限流,第{retry_count}次重试"
                )
                logger.warning(f"[{phone}] ImportContacts rate-limited for {contact_phone}, retry #{retry_count}")
        else:
            await database.update_contact_assign_status(assign_id, "failed", error_reason="号码未注册TG或无法添加")
            logger.warning(f"[{phone}] Cannot find user for {contact_phone}")
            await _restrict_if_never_resolves(phone, account_id)
        return

    # Found user via fallback, add via AddContactRequest with proper InputUser
    _note_phone_resolved(account_id)
    input_user = InputUser(user_id=user.id, access_hash=user.access_hash)
    await tg_client(AddContactRequest(
        id=input_user,
        first_name=getattr(user, "first_name", "") or contact_phone,
        last_name=getattr(user, "last_name", "") or "",
        phone=contact_phone,
        add_phone_privacy_exception=True,
    ))
    await database.upsert_contact(
        tg_account_id=account_id, user_id=user.id,
        access_hash=getattr(user, "access_hash", None),
        first_name=getattr(user, "first_name", None),
        last_name=getattr(user, "last_name", None),
        nickname=getattr(user, "first_name", ""),
        username=getattr(user, "username", None),
        phone_number=contact_phone, source="import", node_id=node_id,
    )
    await database.update_contact_assign_status(assign_id, "success", result_user_id=user.id)
    logger.info(f"[{phone}] Added contact via fallback: {contact_phone} -> {user.id}")


async def _add_one_by_username(tg_client, phone: str, account_id: int, node_id: str,
                                item: dict, contact_username: str):
    """Add a single contact by username: get_entity + AddContactRequest."""
    assign_id = item["id"]
    username = contact_username.strip()
    if username.startswith("@"):
        username = username[1:]

    entity = await tg_client.get_entity(username)
    if not entity:
        await database.update_contact_assign_status(assign_id, "failed", error_reason="用户不存在")
        return

    input_user = InputUser(user_id=entity.id, access_hash=entity.access_hash)
    await tg_client(AddContactRequest(
        id=input_user,
        first_name=getattr(entity, "first_name", "") or username,
        last_name=getattr(entity, "last_name", "") or "",
        phone="",
        add_phone_privacy_exception=True,
    ))
    await database.upsert_contact(
        tg_account_id=account_id, user_id=entity.id,
        access_hash=getattr(entity, "access_hash", None),
        first_name=getattr(entity, "first_name", None),
        last_name=getattr(entity, "last_name", None),
        nickname=getattr(entity, "first_name", ""),
        username=getattr(entity, "username", None),
        source="import", node_id=node_id,
    )
    await database.update_contact_assign_status(assign_id, "success", result_user_id=entity.id)
    logger.info(f"[{phone}] Added contact by username: {username} -> {entity.id}")


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
                            access_hash=getattr(entity, "access_hash", None),
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
                    await _react_add_failure(str(e), phone, account_id)

        username_tasks = [add_by_username(item) for item in username_items]
        await asyncio.gather(*username_tasks, return_exceptions=True)

    # Handle phone-based contacts via ImportContactsRequest
    if not phone_items:
        return

    contacts = []
    item_map = {}
    for i, item in enumerate(phone_items):
        contact_phone = _normalize_phone(item["contact_phone"])
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
                    access_hash=getattr(tg_user, "access_hash", None),
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
        account_hit_limit = False
        for i, item in item_map.items():
            if i not in imported_client_ids:
                if i in retry_contacts:
                    retry_count = await database.increment_retry_count(item["id"])
                    if retry_count >= MAX_RETRY_COUNT and not account_hit_limit:
                        account_hit_limit = True
                        await database.mark_account_restricted(account_id, phone)
                        await database.fail_pending_contacts_for_account(
                            account_id, node_id,
                            f"账号被限制(批量添加好友连续{retry_count}次被限流)"
                        )
                        logger.warning(
                            f"[{phone}] Account restricted: batch import rate-limited {retry_count} times"
                        )
                        break
                    else:
                        await database.update_contact_assign_status(
                            item["id"], "pending", error_reason=f"被限流,第{retry_count}次重试"
                        )
                else:
                    await database.update_contact_assign_status(
                        item["id"], "failed", error_reason="号码未注册TG"
                    )

        logger.info(
            f"[{phone}] contact_import: {len(result.imported or [])}/{len(contacts)} success, "
            f"{len(retry_contacts)} retry{' -> RESTRICTED' if account_hit_limit else ''}"
        )
    except Exception as e:
        logger.error(f"[{phone}] ImportContactsRequest failed: {e}")
        await _react_add_failure(str(e), phone, account_id)
        for i, item in item_map.items():
            await database.update_contact_assign_status(
                item["id"], "failed", error_reason=str(e)[:500]
            )
