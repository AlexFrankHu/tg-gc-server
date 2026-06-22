"""Auto-reply module for automatic message responses.

Two triggers:
1. Incoming message (state=0): reply to friend's message in real-time.
2. Periodic polling (state=1~8): proactively send messages based on rules.
"""
import asyncio
import logging
import re
import random
import tempfile
import os
from datetime import datetime, timedelta, timezone

import httpx
import aiomysql
from telethon.tl.types import User
from telethon.errors import FloodWaitError

import config
from config import to_beijing
import database
import client_manager

logger = logging.getLogger(__name__)

# Official / excluded Telegram IDs (extensible)
OFFICIAL_IDS = {777000}

# Configuration (from config module)
REPLY_API_URL = getattr(config, 'REPLY_API_URL', 'http://127.0.0.1:8000/generate-reply')
POLL_INTERVAL = getattr(config, 'AUTO_REPLY_INTERVAL', 300)  # seconds


# ---------------------------------------------------------------------------
# Trigger 1: Incoming message auto-reply (state=0)
# ---------------------------------------------------------------------------

async def handle_incoming_message(phone: str, event, client):
    """Handle incoming message for auto-reply (state=0)."""
    try:
        msg = event.message
        if not msg or msg.out:
            return

        user_id = msg.sender_id
        if not user_id:
            logger.debug(f"[{phone}] [AutoReply] 跳过: sender_id 为空")
            return

        logger.info(f"[{phone}] [AutoReply] 收到消息, sender_id={user_id}")

        # Exclude official IDs
        if user_id in OFFICIAL_IDS:
            logger.info(f"[{phone}] [AutoReply] 跳过: 官方ID {user_id}")
            return

        # Exclude bots
        try:
            sender = await event.get_sender()
            if not sender or not isinstance(sender, User):
                logger.info(f"[{phone}] [AutoReply] 跳过: sender不是User类型")
                return
            if sender.bot:
                logger.info(f"[{phone}] [AutoReply] 跳过: 机器人 {user_id}")
                return
        except Exception as e:
            logger.warning(f"[{phone}] [AutoReply] 获取sender失败: {e}")
            return

        # Account info
        account = await database.get_account_by_phone(phone)
        if not account:
            logger.info(f"[{phone}] [AutoReply] 跳过: 账号不存在")
            return
        if not account.get('auto_reply', 1):
            logger.info(f"[{phone}] [AutoReply] 跳过: 账号未开启自动回复")
            return
        if account.get('is_restricted', 0):
            logger.info(f"[{phone}] [AutoReply] 跳过: 账号被限制")
            return
        account_id = account['id']

        # Contact info (save_realtime_message already completed before this)
        contact = await _get_contact(account_id, user_id)
        if not contact:
            logger.info(f"[{phone}] [AutoReply] 跳过: 好友 {user_id} 不在联系人表中")
            return
        if not contact.get('auto_reply', 1):
            logger.info(f"[{phone}] [AutoReply] 跳过: 好友 {user_id} 未开启自动回复")
            return
        if contact.get('source') != 'import':
            logger.info(f"[{phone}] [AutoReply] 跳过: 好友 {user_id} 非后台导入好友")
            return

        # Build context & call API — use TG user IDs instead of nicknames
        account_tg_id = str(account.get('tg_user_id') or '')
        friend_tg_id = str(user_id)
        friend_phone_num = contact.get('phone_number')
        logger.info(f"[{phone}] [AutoReply] 准备请求自动回复: state=0, "
                    f"account_id={account_id}, user_id={user_id}, "
                    f"account_tg_id={account_tg_id}, friend_tg_id={friend_tg_id}")

        # Check friend's total incoming message count to decide reply strategy
        friend_msg_count = await _get_friend_sent_count(account_id, user_id)

        if friend_msg_count <= 1:
            # Friend sent <= 1 message: send greeting from tg_greeting
            logger.info(f"[{phone}] [AutoReply] 好友消息数<={friend_msg_count}, 发送广告问候语: user_id={user_id}")
            greeting = await _get_first_greeting()
            if greeting:
                await asyncio.sleep(2)
                try:
                    await _send_greeting_reply(client, phone, account_id, user_id, greeting)
                    logger.info(f"[{phone}] [AutoReply] 广告问候语发送成功: user_id={user_id}")
                    reply_desc = greeting.get('content', '')
                    if greeting.get('image_path'):
                        reply_desc += f" [图片:{greeting['image_path']}]"
                    await database.insert_auto_reply_log(
                        account_phone=phone, account_nickname=account_tg_id,
                        friend_user_id=user_id, friend_nickname=friend_tg_id,
                        friend_phone=friend_phone_num, trigger_type='incoming',
                        state=0, request_params=f'好友消息数={friend_msg_count}<=1, 发送广告问候语',
                        chat_context='', reply_content=reply_desc,
                        send_result='success')
                except Exception as send_err:
                    logger.error(f"[{phone}] [AutoReply] 广告问候语发送失败: user_id={user_id}, error={send_err}")
                    await database.insert_auto_reply_log(
                        account_phone=phone, account_nickname=account_tg_id,
                        friend_user_id=user_id, friend_nickname=friend_tg_id,
                        friend_phone=friend_phone_num, trigger_type='incoming',
                        state=0, request_params=f'好友消息数={friend_msg_count}<=1, 发送广告问候语',
                        chat_context='', reply_content=greeting.get('content', ''),
                        send_result='failed', error_reason=str(send_err))
                    if 'PRIVACY_PREMIUM_REQUIRED' in str(send_err):
                        await _disable_contact_auto_reply(account_id, user_id, phone)
                    raise
            else:
                logger.info(f"[{phone}] [AutoReply] 无有效广告问候语, 跳过: user_id={user_id}")
                await database.insert_auto_reply_log(
                    account_phone=phone, account_nickname=account_tg_id,
                    friend_user_id=user_id, friend_nickname=friend_tg_id,
                    friend_phone=friend_phone_num, trigger_type='incoming',
                    state=0, request_params=f'好友消息数={friend_msg_count}<=1, 发送广告问候语',
                    chat_context='', reply_content=None,
                    send_result='no_reply', error_reason='无有效广告问候语')
        else:
            # Friend sent > 1 message: call auto-reply API
            chat_context = await _build_chat_context(account_id, user_id, account_tg_id, friend_tg_id)

            request_params_str = f"state=0, agent_gender=1, customer_gender=2, my_nickname={account_tg_id}, customer_name={friend_tg_id}"

            reply, api_error = await _get_reply_content(
                state=0,
                my_nickname=account_tg_id,
                customer_name=friend_tg_id,
                chat_context=chat_context,
            )
            if reply:
                await asyncio.sleep(2)  # brief delay for naturalness
                try:
                    await _send_auto_reply(client, phone, account_id, user_id, reply,
                                           my_nickname=account_tg_id, friend_nickname=friend_tg_id,
                                           friend_phone=friend_phone_num)
                    logger.info(f"[{phone}] [AutoReply] 自动回复成功: user_id={user_id}, state=0")
                    await database.insert_auto_reply_log(
                        account_phone=phone, account_nickname=account_tg_id,
                        friend_user_id=user_id, friend_nickname=friend_tg_id,
                        friend_phone=friend_phone_num, trigger_type='incoming',
                        state=0, request_params=request_params_str,
                        chat_context=chat_context, reply_content=reply,
                        send_result='success')
                except Exception as send_err:
                    logger.error(f"[{phone}] [AutoReply] 发送消息失败: user_id={user_id}, error={send_err}")
                    await database.insert_auto_reply_log(
                        account_phone=phone, account_nickname=account_tg_id,
                        friend_user_id=user_id, friend_nickname=friend_tg_id,
                        friend_phone=friend_phone_num, trigger_type='incoming',
                        state=0, request_params=request_params_str,
                        chat_context=chat_context, reply_content=reply,
                        send_result='failed', error_reason=str(send_err))
                    if 'PRIVACY_PREMIUM_REQUIRED' in str(send_err):
                        await _disable_contact_auto_reply(account_id, user_id, phone)
                    raise
            else:
                result_type = 'api_error' if api_error and 'API' in api_error else 'no_reply'
                logger.warning(f"[{phone}] [AutoReply] API未返回有效回复内容: {api_error}")
                await database.insert_auto_reply_log(
                    account_phone=phone, account_nickname=account_tg_id,
                    friend_user_id=user_id, friend_nickname=friend_tg_id,
                    friend_phone=friend_phone_num, trigger_type='incoming',
                    state=0, request_params=request_params_str,
                    chat_context=chat_context, reply_content=None,
                    send_result=result_type, error_reason=api_error)
    except FloodWaitError as e:
        logger.warning(f"[{phone}] [AutoReply] FloodWait: 需要等待{e.seconds}s后再发消息")
        await asyncio.sleep(e.seconds + 5)
    except Exception as e:
        logger.error(f"[{phone}] [AutoReply] 处理incoming消息异常: {e}")


# ---------------------------------------------------------------------------
# Trigger 2: Periodic polling (state=1~8)
# ---------------------------------------------------------------------------

async def poll_auto_reply():
    """Background task: poll contacts for proactive auto-reply every POLL_INTERVAL seconds."""
    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        try:
            logger.info("Auto-reply poll: starting...")
            await _process_proactive_replies()
            logger.info("Auto-reply poll: done")
        except asyncio.CancelledError:
            logger.warning("Auto-reply poll: CancelledError during processing, will retry next cycle")
            continue
        except Exception as e:
            logger.error(f"Auto-reply poll error: {e}")


async def _process_proactive_replies():
    """Iterate eligible contacts and send proactive messages where state >= 0."""
    contacts = await _get_eligible_contacts()
    if not contacts:
        return

    logger.info(f"Auto-reply poll: {len(contacts)} eligible contacts")

    for row in contacts:
        try:
            phone = row['phone']
            if phone not in client_manager.active_clients:
                continue

            client = client_manager.active_clients[phone]
            account_id = row['account_id']
            user_id = row['user_id']

            last_send_time = row.get('last_send_time')
            last_receive_time = row.get('last_receive_time')

            state = await _calculate_state(account_id, user_id, last_send_time, last_receive_time)
            if state < 0:
                continue

            account_tg_id = str(row.get('account_tg_user_id') or '')
            friend_tg_id = str(user_id)
            friend_phone_num = row.get('phone_number')

            if state == 0:
                # state=0 (polling): friend has sent messages, check count
                friend_msg_count = await _get_friend_sent_count(account_id, user_id)
                if friend_msg_count <= 1:
                    # Friend sent <= 1 message: send greeting from tg_greeting
                    greeting = await _get_first_greeting()
                    if greeting:
                        try:
                            await _send_greeting_reply(client, phone, account_id, user_id, greeting)
                            logger.info(f"[{phone}] 广告问候语发送成功(polling state=0): user_id={user_id}")
                            reply_desc = greeting.get('content', '')
                            if greeting.get('image_path'):
                                reply_desc += f" [图片:{greeting['image_path']}]"
                            await database.insert_auto_reply_log(
                                account_phone=phone, account_nickname=account_tg_id,
                                friend_user_id=user_id, friend_nickname=friend_tg_id,
                                friend_phone=friend_phone_num, trigger_type='polling',
                                state=state, request_params='state=0, 好友消息数<=1, 发送广告问候语',
                                chat_context='', reply_content=reply_desc,
                                send_result='success')
                        except Exception as send_err:
                            logger.error(f"[{phone}] 广告问候语发送失败(polling): user_id={user_id}, error={send_err}")
                            await database.insert_auto_reply_log(
                                account_phone=phone, account_nickname=account_tg_id,
                                friend_user_id=user_id, friend_nickname=friend_tg_id,
                                friend_phone=friend_phone_num, trigger_type='polling',
                                state=state, request_params='state=0, 好友消息数<=1, 发送广告问候语',
                                chat_context='', reply_content=greeting.get('content', ''),
                                send_result='failed', error_reason=str(send_err))
                            if 'PRIVACY_PREMIUM_REQUIRED' in str(send_err):
                                await _disable_contact_auto_reply(account_id, user_id, phone)
                            raise
                    else:
                        logger.info(f"[{phone}] 无有效广告问候语(polling state=0): user_id={user_id}")
                        await database.insert_auto_reply_log(
                            account_phone=phone, account_nickname=account_tg_id,
                            friend_user_id=user_id, friend_nickname=friend_tg_id,
                            friend_phone=friend_phone_num, trigger_type='polling',
                            state=state, request_params='state=0, 好友消息数<=1, 发送广告问候语',
                            chat_context='', reply_content=None,
                            send_result='no_reply', error_reason='无有效广告问候语')
                else:
                    # Friend sent > 1 message: call auto-reply API
                    chat_context = await _build_chat_context(account_id, user_id, account_tg_id, friend_tg_id)
                    request_params_str = f"state=0, agent_gender=1, customer_gender=2, my_nickname={account_tg_id}, customer_name={friend_tg_id}"
                    reply, api_error = await _get_reply_content(
                        state=0,
                        my_nickname=account_tg_id,
                        customer_name=friend_tg_id,
                        chat_context=chat_context,
                    )
                    if reply:
                        try:
                            await _send_auto_reply(client, phone, account_id, user_id, reply,
                                                   my_nickname=account_tg_id, friend_nickname=friend_tg_id,
                                                   friend_phone=friend_phone_num)
                            logger.info(f"[{phone}] 自动回复成功(polling state=0): user_id={user_id}")
                            await database.insert_auto_reply_log(
                                account_phone=phone, account_nickname=account_tg_id,
                                friend_user_id=user_id, friend_nickname=friend_tg_id,
                                friend_phone=friend_phone_num, trigger_type='polling',
                                state=state, request_params=request_params_str,
                                chat_context=chat_context, reply_content=reply,
                                send_result='success')
                        except Exception as send_err:
                            logger.error(f"[{phone}] 发送失败(polling state=0): user_id={user_id}, error={send_err}")
                            await database.insert_auto_reply_log(
                                account_phone=phone, account_nickname=account_tg_id,
                                friend_user_id=user_id, friend_nickname=friend_tg_id,
                                friend_phone=friend_phone_num, trigger_type='polling',
                                state=state, request_params=request_params_str,
                                chat_context=chat_context, reply_content=reply,
                                send_result='failed', error_reason=str(send_err))
                            if 'PRIVACY_PREMIUM_REQUIRED' in str(send_err):
                                await _disable_contact_auto_reply(account_id, user_id, phone)
                            raise
                    else:
                        result_type = 'api_error' if api_error and 'API' in api_error else 'no_reply'
                        await database.insert_auto_reply_log(
                            account_phone=phone, account_nickname=account_tg_id,
                            friend_user_id=user_id, friend_nickname=friend_tg_id,
                            friend_phone=friend_phone_num, trigger_type='polling',
                            state=state, request_params=request_params_str,
                            chat_context=chat_context, reply_content=None,
                            send_result=result_type, error_reason=api_error)
            elif state == 1:
                # state=1: send random opening from tg_opening instead of calling API
                opening = await _get_random_opening()
                if opening:
                    try:
                        opening_content = opening['content']
                        await _send_auto_reply(client, phone, account_id, user_id, opening_content,
                                               my_nickname=account_tg_id, friend_nickname=friend_tg_id,
                                               friend_phone=friend_phone_num)
                        logger.info(f"[{phone}] 主动开场白发送成功: user_id={user_id} (state=1)")
                        await database.insert_auto_reply_log(
                            account_phone=phone, account_nickname=account_tg_id,
                            friend_user_id=user_id, friend_nickname=friend_tg_id,
                            friend_phone=friend_phone_num, trigger_type='polling',
                            state=state, request_params='state=1, 随机主动开场白',
                            chat_context='', reply_content=opening_content,
                            send_result='success')
                    except Exception as send_err:
                        logger.error(f"[{phone}] 主动开场白发送失败: user_id={user_id}, error={send_err}")
                        await database.insert_auto_reply_log(
                            account_phone=phone, account_nickname=account_tg_id,
                            friend_user_id=user_id, friend_nickname=friend_tg_id,
                            friend_phone=friend_phone_num, trigger_type='polling',
                            state=state, request_params='state=1, 随机主动开场白',
                            chat_context='', reply_content=opening_content,
                            send_result='failed', error_reason=str(send_err))
                        if 'PRIVACY_PREMIUM_REQUIRED' in str(send_err):
                            await _disable_contact_auto_reply(account_id, user_id, phone)
                        raise
                else:
                    logger.info(f"[{phone}] 无有效主动开场白, 跳过: user_id={user_id}")
                    await database.insert_auto_reply_log(
                        account_phone=phone, account_nickname=account_tg_id,
                        friend_user_id=user_id, friend_nickname=friend_tg_id,
                        friend_phone=friend_phone_num, trigger_type='polling',
                        state=state, request_params='state=1, 随机主动开场白',
                        chat_context='', reply_content=None,
                        send_result='no_reply', error_reason='无有效主动开场白')
            else:
                # state>=2: call auto-reply API
                chat_context = await _build_chat_context(account_id, user_id, account_tg_id, friend_tg_id)

                request_params_str = f"state={state}, agent_gender=1, customer_gender=2, my_nickname={account_tg_id}, customer_name={friend_tg_id}"

                reply, api_error = await _get_reply_content(
                    state=state,
                    my_nickname=account_tg_id,
                    customer_name=friend_tg_id,
                    chat_context=chat_context,
                )
                if reply:
                    try:
                        await _send_auto_reply(client, phone, account_id, user_id, reply,
                                               my_nickname=account_tg_id, friend_nickname=friend_tg_id,
                                               friend_phone=friend_phone_num)
                        logger.info(f"[{phone}] Proactive auto-reply to {user_id} (state={state})")
                        await database.insert_auto_reply_log(
                            account_phone=phone, account_nickname=account_tg_id,
                            friend_user_id=user_id, friend_nickname=friend_tg_id,
                            friend_phone=friend_phone_num, trigger_type='polling',
                            state=state, request_params=request_params_str,
                            chat_context=chat_context, reply_content=reply,
                            send_result='success')
                    except Exception as send_err:
                        logger.error(f"[{phone}] 发送失败: user_id={user_id}, error={send_err}")
                        await database.insert_auto_reply_log(
                            account_phone=phone, account_nickname=account_tg_id,
                            friend_user_id=user_id, friend_nickname=friend_tg_id,
                            friend_phone=friend_phone_num, trigger_type='polling',
                            state=state, request_params=request_params_str,
                            chat_context=chat_context, reply_content=reply,
                            send_result='failed', error_reason=str(send_err))
                        if 'PRIVACY_PREMIUM_REQUIRED' in str(send_err):
                            await _disable_contact_auto_reply(account_id, user_id, phone)
                        raise
                else:
                    result_type = 'api_error' if api_error and 'API' in api_error else 'no_reply'
                    await database.insert_auto_reply_log(
                        account_phone=phone, account_nickname=account_tg_id,
                        friend_user_id=user_id, friend_nickname=friend_tg_id,
                        friend_phone=friend_phone_num, trigger_type='polling',
                        state=state, request_params=request_params_str,
                        chat_context=chat_context, reply_content=None,
                        send_result=result_type, error_reason=api_error)
        except FloodWaitError as e:
            logger.warning(f"[{phone}] FloodWait: need to wait {e.seconds}s, pausing...")
            await asyncio.sleep(min(e.seconds + 5, 300))  # wait as Telegram requires, cap at 5min
        except Exception as e:
            logger.error(f"Auto-reply poll error for user_id={row.get('user_id')}: {e}")


# ---------------------------------------------------------------------------
# State calculation
# ---------------------------------------------------------------------------

async def _calculate_state(account_id: int, chat_id: int,
                           last_send_time, last_receive_time) -> int:
    """Return the state (0~8) for proactive messaging, or -1 to skip."""
    now = datetime.now()

    if last_receive_time is None:
        # Friend has never sent a message
        if last_send_time is None:
            return 1

        send_count = await _get_outgoing_message_count(account_id, chat_id)
        if send_count == 0:
            return 1

        hours_since_last = (now - last_send_time).total_seconds() / 3600

        if send_count == 1 and hours_since_last >= 1:
            return 2
        if send_count == 2 and hours_since_last >= 3:
            return 3
        if send_count == 3 and hours_since_last >= 24:
            return 4
        return -1  # send_count > 3 or interval too short
    else:
        # Friend has sent messages before
        if last_send_time is None:
            return 0

        if last_send_time < last_receive_time:
            return 0

        hours_since_last = (now - last_send_time).total_seconds() / 3600

        # Check last 5 messages to count consecutive outgoing from the newest
        last_messages = await _get_last_messages(account_id, chat_id, 5)
        consecutive_out = 0
        for m in last_messages:
            if m.get('is_outgoing'):
                consecutive_out += 1
            else:
                break

        if consecutive_out >= 5:
            return -1
        if consecutive_out == 4 and 48 <= hours_since_last <= 72:
            return 8
        if consecutive_out == 3 and 24 <= hours_since_last <= 48:
            return 7
        if consecutive_out == 2 and 12 <= hours_since_last <= 24:
            return 6
        if consecutive_out == 1 and 3 <= hours_since_last <= 12:
            return 5
        return -1


# ---------------------------------------------------------------------------
# Reply API
# ---------------------------------------------------------------------------

async def _get_reply_content(state: int, my_nickname: str,
                             customer_name: str, chat_context: str) -> tuple:
    """Call auto-reply API and return (reply_text, error_reason).
    On success: (reply_text, None). On failure: (None, error_reason).
    Retries up to 3 times on failure."""
    body = {
        "state": state,
        "agent_gender": 1,       # female (fixed)
        "customer_gender": 2,    # unknown (fixed)
        "my_nickname": my_nickname,
        "customer_name": customer_name,
        "chat_context": chat_context,
    }
    logger.info(f"[AutoReply] 请求地址: {REPLY_API_URL}")
    logger.info(f"[AutoReply] 请求参数: state={state}, agent_gender=1, customer_gender=2, "
                 f"my_nickname={my_nickname}, customer_name={customer_name}")
    logger.info(f"[AutoReply] chat_context:\n{chat_context}")

    max_retries = 3
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30) as http_client:
                resp = await http_client.post(REPLY_API_URL, json=body)
                logger.info(f"[AutoReply] 第{attempt}次请求 响应状态码: {resp.status_code}")
                logger.info(f"[AutoReply] 响应内容: {resp.text}")
                if resp.status_code == 200:
                    data = resp.json()
                    reply = data.get("reply")
                    logger.info(f"[AutoReply] 解析回复内容: {reply}")
                    if not reply or not reply.strip():
                        logger.info("[AutoReply] 回复内容为空，不发送")
                        return (None, "API返回回复内容为空")
                    return (reply, None)
                last_error = f"API返回非200: status={resp.status_code}, body={resp.text[:500]}"
                logger.warning(f"[AutoReply] 第{attempt}次请求失败: {last_error}")
        except httpx.ConnectTimeout as e:
            last_error = f"API连接超时(ConnectTimeout): 无法连接到 {REPLY_API_URL}, 第{attempt}次"
            logger.error(f"[AutoReply] {last_error}")
        except httpx.ReadTimeout as e:
            last_error = f"API读取超时(ReadTimeout): 服务器响应超时, 第{attempt}次"
            logger.error(f"[AutoReply] {last_error}")
        except httpx.ConnectError as e:
            last_error = f"API连接失败(ConnectError): 无法连接到 {REPLY_API_URL}, 错误: {type(e).__name__}: {e}, 第{attempt}次"
            logger.error(f"[AutoReply] {last_error}")
        except httpx.PoolTimeout as e:
            last_error = f"API连接池超时(PoolTimeout): 连接池已满, 第{attempt}次"
            logger.error(f"[AutoReply] {last_error}")
        except httpx.TimeoutException as e:
            last_error = f"API超时({type(e).__name__}): {e}, 第{attempt}次"
            logger.error(f"[AutoReply] {last_error}")
        except Exception as e:
            last_error = f"API请求异常({type(e).__name__}): {e}, 第{attempt}次"
            logger.error(f"[AutoReply] {last_error}")

        if attempt < max_retries:
            await asyncio.sleep(2 * attempt)  # backoff: 2s, 4s

    error_msg = f"API请求失败(重试{max_retries}次): {last_error}"
    logger.error(f"[AutoReply] {error_msg}")
    return (None, error_msg)


# ---------------------------------------------------------------------------
# Send message helper
# ---------------------------------------------------------------------------

# Regex to match [AIMG:url] tags
_AIMG_PATTERN = re.compile(r'\[AIMG:(.*?)\]')


def _parse_reply_content(text: str):
    """Parse reply text for [AIMG:url] tags.
    Returns (image_urls: list[str], remaining_text: str).
    """
    image_urls = _AIMG_PATTERN.findall(text)
    remaining = _AIMG_PATTERN.sub('', text).strip()
    # Clean up extra blank lines left after removing AIMG tags
    remaining = re.sub(r'\n\s*\n', '\n', remaining).strip()
    return image_urls, remaining


async def _download_image(url: str) -> str | None:
    """Download image from URL to a temp file, return the file path."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                # Determine extension from URL or content-type
                ext = '.jpg'
                ct = resp.headers.get('content-type', '')
                if 'png' in ct:
                    ext = '.png'
                elif 'gif' in ct:
                    ext = '.gif'
                elif 'webp' in ct:
                    ext = '.webp'
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
                tmp.write(resp.content)
                tmp.close()
                return tmp.name
            else:
                logger.warning(f"[AutoReply] 下载图片失败: {url}, status={resp.status_code}")
    except Exception as e:
        logger.error(f"[AutoReply] 下载图片异常: {url}, error={e}")
    return None


async def _disable_contact_auto_reply(account_id: int, user_id: int, phone: str):
    """Disable auto_reply for a specific contact when PRIVACY_PREMIUM_REQUIRED."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE tg_contact SET auto_reply = 0 WHERE tg_account_id = %s AND user_id = %s",
                    (account_id, user_id)
                )
        logger.info(f"[{phone}] [AutoReply] 已关闭好友 {user_id} 的自动回复 (PRIVACY_PREMIUM_REQUIRED)")
    except Exception as e:
        logger.error(f"[{phone}] [AutoReply] 关闭好友自动回复失败: {e}")


async def _send_auto_reply(client, phone: str, account_id: int,
                           user_id: int, text: str,
                           my_nickname: str = None, friend_nickname: str = None,
                           friend_phone: str = None):
    """Send a message via Telethon, save to DB, and update last_send_time.
    Supports [AIMG:url] tags: sends images first, then remaining text."""
    try:
        image_urls, remaining_text = _parse_reply_content(text)
        last_sent_msg = None

        # Send images first
        for img_url in image_urls:
            try:
                logger.info(f"[{phone}] [AutoReply] 下载图片: {img_url}")
                img_path = await _download_image(img_url)
                if img_path:
                    try:
                        sent_msg = await client.send_file(user_id, img_path)
                    except FloodWaitError as e:
                        logger.warning(f"[{phone}] [AutoReply] FloodWait发送图片: 等待{e.seconds}s")
                        await asyncio.sleep(e.seconds + 5)
                        sent_msg = await client.send_file(user_id, img_path)
                    last_sent_msg = sent_msg
                    logger.info(f"[{phone}] [AutoReply] 图片发送成功: user_id={user_id}, url={img_url}")
                    await _save_sent_message(phone, account_id, user_id, sent_msg, 'photo', f'[AIMG:{img_url}]')
                    try:
                        os.unlink(img_path)
                    except Exception:
                        pass
                    await asyncio.sleep(2)  # brief delay between images
                else:
                    logger.warning(f"[{phone}] [AutoReply] 图片下载失败，跳过: {img_url}")
            except Exception as e:
                logger.error(f"[{phone}] [AutoReply] 发送图片失败: url={img_url}, error={e}")
                await _write_send_fail_log(phone, account_id, my_nickname, user_id,
                                          friend_nickname, friend_phone, 'photo',
                                          f'[AIMG:{img_url}]', str(e))

        # Send remaining text (only if non-empty)
        if remaining_text:
            try:
                sent_msg = await client.send_message(user_id, remaining_text)
            except FloodWaitError as e:
                logger.warning(f"[{phone}] [AutoReply] FloodWait发送文字: 等待{e.seconds}s")
                await asyncio.sleep(e.seconds + 5)
                try:
                    sent_msg = await client.send_message(user_id, remaining_text)
                except Exception as e2:
                    raise Exception(f"FloodWait重试后仍失败: {e2}") from e2
            last_sent_msg = sent_msg
            logger.info(f"[{phone}] [AutoReply] 文字发送成功: user_id={user_id}, text={remaining_text[:80]}...")
            await _save_sent_message(phone, account_id, user_id, sent_msg, 'text', remaining_text)

        # Update last_send_time with the last sent message
        if last_sent_msg:
            try:
                send_time = to_beijing(last_sent_msg.date) if last_sent_msg.date else datetime.now()
                async with database.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """UPDATE tg_contact
                               SET last_send_time = %s
                               WHERE tg_account_id = %s AND user_id = %s
                                 AND (last_send_time IS NULL OR last_send_time < %s)""",
                            (send_time, account_id, user_id, send_time),
                        )
                logger.info(f"[{phone}] [AutoReply] last_send_time 已更新")
            except Exception as e:
                logger.error(f"[{phone}] [AutoReply] 更新 last_send_time 失败: {e}")

    except Exception as e:
        logger.error(f"[{phone}] [AutoReply] 发送消息失败: user_id={user_id}, error={e}")
        await _write_send_fail_log(phone, account_id, my_nickname, user_id,
                                  friend_nickname, friend_phone, 'text',
                                  text, str(e))
        raise


async def _save_sent_message(phone: str, account_id: int, user_id: int,
                             sent_msg, content_type: str, content: str):
    """Save a sent message to tg_chat_message table."""
    try:
        send_time = to_beijing(sent_msg.date) if sent_msg.date else datetime.now()
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO tg_chat_message
                       (tg_account_id, chat_id, message_id, sender_user_id,
                        is_outgoing, send_time, content_type, text_content, create_time)
                       VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NOW())
                       ON DUPLICATE KEY UPDATE text_content = VALUES(text_content)""",
                    (account_id, user_id, sent_msg.id, None,
                     send_time, content_type, content),
                )
        logger.info(f"[{phone}] [AutoReply] 消息已录入数据库: msg_id={sent_msg.id}, type={content_type}")
        await database.increment_msg_count(account_id, is_outgoing=True)
        await database.increment_contact_msg_count(account_id, user_id, is_outgoing=True)
    except Exception as e:
        logger.error(f"[{phone}] [AutoReply] 录入消息到数据库失败: {e}")


async def _write_send_fail_log(phone, account_id, my_nickname, user_id,
                                friend_nickname, friend_phone, content_type,
                                content, error_reason):
    """Write a send failure log to the database.
    If the account has 3+ FloodWait failures, mark it as restricted."""
    try:
        await database.insert_send_fail_log(
            phone=phone, tg_account_id=account_id, nickname=my_nickname,
            user_id=user_id, friend_nickname=friend_nickname,
            friend_phone=friend_phone, content_type=content_type,
            content=content[:500] if content else None,
            error_reason=error_reason[:500] if error_reason else None
        )
        logger.info(f"[{phone}] [AutoReply] 发送失败日志已写入: user_id={user_id}")
        # Check if account should be marked as restricted
        if error_reason and 'Too many requests' in error_reason:
            await _check_and_restrict_account(phone, account_id)
    except Exception as e:
        logger.error(f"[{phone}] [AutoReply] 写入发送失败日志失败: {e}")


async def _check_and_restrict_account(phone, account_id):
    """If the account has 3+ FloodWait send failures, mark it as restricted,
    logout the account, and send TG notification."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT COUNT(*) AS cnt FROM tg_send_fail_log
                       WHERE phone = %s AND error_reason LIKE '%%Too many requests%%'""",
                    (phone,)
                )
                row = await cur.fetchone()
                fail_count = row[0] if row else 0
                if fail_count >= 3:
                    await cur.execute(
                        "UPDATE tg_telethon_account SET is_restricted = 1, status = 'restricted', update_time = NOW() WHERE id = %s",
                        (account_id,)
                    )
                    logger.warning(f"[{phone}] 账号已被标记为限制 (FloodWait失败{fail_count}次), 正在登出...")
                    # Logout the account and release resources
                    try:
                        client = client_manager.active_clients.pop(phone, None)
                        if client:
                            await client.disconnect()
                            logger.info(f"[{phone}] 账号已登出并释放资源")
                    except Exception as logout_err:
                        logger.error(f"[{phone}] 登出账号失败: {logout_err}")
                    # Send TG notification
                    await notify.send_notification(
                        "账号被限制",
                        f"账号: +{phone}\n原因: Too many requests 发送失败{fail_count}次\n状态: 已自动登出并标记为限制"
                    )
    except Exception as e:
        logger.error(f"[{phone}] 检查账号限制状态失败: {e}")


# ---------------------------------------------------------------------------
# Chat context builder
# ---------------------------------------------------------------------------

# Mapping from DB content_type to chat_context tag for media messages
_MEDIA_TAG_MAP = {
    'photo':    'IMG',
    'video':    'VIDEO',
    'video_note': 'VIDEONOTE',
    'gif':      'GIF',
    'animation': 'GIF',
    'voice':    'VOICE',
    'audio':    'AUDIO',
    'document': 'FILE',
    'sticker':  'STICKER',
    'custom_emoji': 'CUSTOMEMOJI',
    'geo':      'LOCATION',
    'geo_live': 'LOCATIONLIVE',
    'contact':  'CONTACT',
    'poll':     'POLL',
    'dice':     'DICE',
    'invoice':  'INVOICE',
    'game':     'GAME',
    'story':    'STORY',
}


def _format_message_content(msg: dict) -> str:
    """Format a single message's content for chat_context.
    Text messages return raw text_content.
    Media messages return [TAG:placeholder] or [IMG:url] for photos."""
    content_type = (msg.get('content_type') or 'text').lower()
    text_content = msg.get('text_content') or ''

    if content_type == 'text':
        return text_content

    tag = _MEDIA_TAG_MAP.get(content_type)
    if not tag:
        # Unknown media type
        return '[UNKNOWN:placeholder]'

    if tag == 'IMG':
        # For photos: try to extract URL from text_content if available
        # Auto-reply sent images store content as [AIMG:url]
        aimg_match = _AIMG_PATTERN.search(text_content)
        if aimg_match:
            return f'[IMG:{aimg_match.group(1)}]'
        # No URL available, use placeholder
        return '[IMG:placeholder]'

    # All other media types use placeholder
    return f'[{tag}:placeholder]'


async def _build_chat_context(account_id: int, chat_id: int,
                              my_nickname: str, friend_nickname: str,
                              limit: int = 60) -> str:
    """Build context string: {nickname}[{time}]:{content} per line."""
    messages = await _get_chat_messages(account_id, chat_id, limit)
    if not messages:
        return ""

    messages.reverse()  # chronological order

    lines = []
    for msg in messages:
        nickname = my_nickname if msg.get('is_outgoing') else friend_nickname
        t = msg.get('send_time')
        time_str = t.strftime('%Y-%m-%d %H:%M:%S') if t else ''
        content = _format_message_content(msg)
        if content:
            lines.append(f"{nickname}[{time_str}]:{content}")
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def _get_contact(account_id: int, user_id: int):
    async with database.pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM tg_contact WHERE tg_account_id = %s AND user_id = %s",
                (account_id, user_id),
            )
            return await cur.fetchone()


async def _get_eligible_contacts() -> list:
    """Contacts eligible for proactive auto-reply polling."""
    placeholders = ','.join(['%s'] * len(OFFICIAL_IDS)) if OFFICIAL_IDS else '0'
    params = list(OFFICIAL_IDS) if OFFICIAL_IDS else []

    sql = f"""
        SELECT c.*,
               a.id AS account_id, a.phone,
               a.nickname AS account_nickname,
               a.tg_user_id AS account_tg_user_id
        FROM tg_contact c
        JOIN tg_telethon_account a ON c.tg_account_id = a.id
        WHERE c.auto_reply = 1
          AND c.is_bot = 0
          AND c.source = 'import'
          AND c.user_id NOT IN ({placeholders})
          AND a.auto_reply = 1
          AND a.status = 'online'
          AND (a.is_deleted = 0 OR a.is_deleted IS NULL)
          AND (a.is_restricted = 0 OR a.is_restricted IS NULL)
          AND (
              c.last_send_time IS NULL
              OR c.last_receive_time IS NULL
              OR (
                  TIMESTAMPDIFF(HOUR, c.last_send_time, NOW()) < 72
                  AND c.last_send_time > c.last_receive_time
              )
          )
    """
    async with database.pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()


async def _get_outgoing_message_count(account_id: int, chat_id: int) -> int:
    async with database.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM tg_chat_message "
                "WHERE tg_account_id = %s AND chat_id = %s AND is_outgoing = 1",
                (account_id, chat_id),
            )
            row = await cur.fetchone()
            return row[0] if row else 0


async def _get_last_messages(account_id: int, chat_id: int, limit: int = 5) -> list:
    """Last N messages (newest first)."""
    async with database.pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT is_outgoing, send_time, text_content "
                "FROM tg_chat_message "
                "WHERE tg_account_id = %s AND chat_id = %s "
                "ORDER BY send_time DESC, message_id DESC LIMIT %s",
                (account_id, chat_id, limit),
            )
            return await cur.fetchall()


async def _get_chat_messages(account_id: int, chat_id: int, limit: int = 20) -> list:
    """Recent messages for context building (newest first)."""
    async with database.pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT is_outgoing, send_time, text_content, content_type "
                "FROM tg_chat_message "
                "WHERE tg_account_id = %s AND chat_id = %s "
                "ORDER BY send_time DESC, message_id DESC LIMIT %s",
                (account_id, chat_id, limit),
            )
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Opening message & greeting helpers
# ---------------------------------------------------------------------------

async def _get_random_opening() -> dict | None:
    """Get a random enabled opening message from tg_opening table."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT id, content FROM tg_opening WHERE is_enabled = 1"
                )
                rows = await cur.fetchall()
                if rows:
                    return random.choice(rows)
                return None
    except Exception as e:
        logger.error(f"[AutoReply] 获取主动开场白失败: {e}")
        return None


async def _get_first_greeting() -> dict | None:
    """Get the first enabled greeting from tg_greeting table."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT id, content, image_path FROM tg_greeting WHERE is_enabled = 1 ORDER BY sort_order ASC, id ASC LIMIT 1"
                )
                return await cur.fetchone()
    except Exception as e:
        logger.error(f"[AutoReply] 获取广告问候语失败: {e}")
        return None


async def _get_friend_sent_count(account_id: int, user_id: int) -> int:
    """Get the total count of messages sent by the friend (non-outgoing) to this account."""
    try:
        async with database.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT COUNT(*) FROM tg_chat_message "
                    "WHERE tg_account_id = %s AND chat_id = %s AND is_outgoing = 0",
                    (account_id, user_id),
                )
                row = await cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.error(f"[AutoReply] 获取好友发送消息数失败: {e}")
        return 0


async def _send_greeting_reply(client, phone: str, account_id: int, user_id: int, greeting: dict):
    """Send a greeting (from tg_greeting) as a reply. Supports image+caption.
    Saves chat records (split text + photo if both exist) and updates last_send_time."""
    content = greeting.get('content', '')
    image_path = greeting.get('image_path', '')
    beijing_tz = timezone(timedelta(hours=8))

    if not content and not image_path:
        return

    sent_msg = None
    has_image = False

    if image_path:
        actual_path = "/home/ubuntu/telegram-project/uploadPath" + image_path.replace("/profile", "", 1)
        if os.path.exists(actual_path):
            sent_msg = await client.send_file(user_id, actual_path, caption=content or '')
            has_image = True
            logger.info(f"[{phone}] [AutoReply] 广告问候语(图片+文字)发送成功: user_id={user_id}")
        else:
            logger.warning(f"[{phone}] [AutoReply] 广告问候语图片不存在: {actual_path}, 仅发送文字")
            if content:
                sent_msg = await client.send_message(user_id, content)
    else:
        if content:
            sent_msg = await client.send_message(user_id, content)
            logger.info(f"[{phone}] [AutoReply] 广告问候语(文字)发送成功: user_id={user_id}")

    if not sent_msg:
        return

    # Save to database: split text and image into separate records
    send_time = sent_msg.date.astimezone(beijing_tz) if sent_msg.date else datetime.now(beijing_tz)
    msg_id = sent_msg.id

    async with database.pool.acquire() as conn:
        async with conn.cursor() as cur:
            if content:
                await cur.execute(
                    """INSERT INTO tg_chat_message
                       (tg_account_id, chat_id, message_id, sender_user_id,
                        is_outgoing, send_time, content_type, text_content, create_time)
                       VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NOW())
                       ON DUPLICATE KEY UPDATE text_content = VALUES(text_content)""",
                    (account_id, user_id, msg_id, None,
                     send_time, 'text', content),
                )
                await database.increment_msg_count(account_id, is_outgoing=True)
                await database.increment_contact_msg_count(account_id, user_id, is_outgoing=True)

            if has_image:
                img_msg_id = msg_id + 100000000 if content else msg_id
                await cur.execute(
                    """INSERT INTO tg_chat_message
                       (tg_account_id, chat_id, message_id, sender_user_id,
                        is_outgoing, send_time, content_type, text_content, create_time)
                       VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NOW())
                       ON DUPLICATE KEY UPDATE text_content = VALUES(text_content)""",
                    (account_id, user_id, img_msg_id, None,
                     send_time, 'photo', image_path),
                )
                await database.increment_msg_count(account_id, is_outgoing=True)
                await database.increment_contact_msg_count(account_id, user_id, is_outgoing=True)

            # Update last_send_time
            await cur.execute(
                """UPDATE tg_contact SET last_send_time = %s
                   WHERE tg_account_id = %s AND user_id = %s""",
                (send_time, account_id, user_id)
            )

    logger.info(f"[{phone}] [AutoReply] 广告问候语聊天记录已写入: user_id={user_id}")
