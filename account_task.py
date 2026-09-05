"""
Account task executor.

后台把「修改昵称 / 修改头像 / 修改 2FA 密码」写入 tg_account_task(status=pending),
本模块每 15s 轮询本节点账号的待办任务, 用已在线的 Telethon client 执行并回写结果.
单个账号失败只记录 error_reason, 不影响其他账号.
"""

import asyncio
import json
import logging
import os
import tempfile

import httpx
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.photos import (
    DeletePhotosRequest,
    GetUserPhotosRequest,
    UploadProfilePhotoRequest,
)
from telethon.tl.types import InputPhoto, PhotoEmpty

import config
import database
import node_manager
import client_manager
import tg_errors
import watchdog

logger = logging.getLogger(__name__)

TASK_POLL_INTERVAL = 15
BACKEND_BASE_URL = f"http://{config.DB_HOST}:8809"
# 同一节点同时执行的任务数上限, 避免一次下发上千任务时把节点/TG 打爆
MAX_CONCURRENCY = 5


async def poll_account_tasks():
    """Background loop: process pending account tasks every 15 seconds."""
    while True:
        try:
            await asyncio.sleep(TASK_POLL_INTERVAL)
            await _process_pending_tasks()
            watchdog.ping()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Account task poll error: {e}")


async def _process_pending_tasks():
    node_id = node_manager.NODE_ID
    tasks = await database.get_pending_account_tasks(node_id)
    if not tasks:
        return
    logger.info(f"[AccountTask] 本节点待执行任务 {len(tasks)} 个")
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def _run(task):
        async with sem:
            await _execute_task(task)

    await asyncio.gather(*(_run(t) for t in tasks), return_exceptions=True)


async def _execute_task(task: dict):
    task_id = task["id"]
    phone = task.get("phone") or ""
    task_type = task.get("task_type")
    try:
        param = json.loads(task.get("param") or "{}")
    except Exception:
        param = {}

    account = await database.get_account_by_phone(phone)
    if not account or account.get("is_deleted"):
        await database.finish_account_task(task_id, "failed", "账号不存在")
        return
    if database.is_account_blocked(account):
        await database.finish_account_task(task_id, "failed", "账号已受限/冻结, 跳过")
        return
    client = client_manager.active_clients.get(phone)
    if client is None or not client.is_connected():
        await database.finish_account_task(task_id, "failed", "账号未在本节点在线")
        return

    try:
        if task_type == "nickname":
            await _do_nickname(client, phone, param)
        elif task_type == "avatar":
            await _do_avatar(client, phone, param)
        elif task_type == "twofa":
            await _do_twofa(client, phone, account, param)
        else:
            await database.finish_account_task(task_id, "failed", f"未知任务类型 {task_type}")
            return
        await database.finish_account_task(task_id, "success", None)
        logger.info(f"[{phone}] 账号任务 {task_type} 执行成功 (task_id={task_id})")
    except Exception as e:
        err = str(e)
        if tg_errors.is_frozen_error(err):
            await database.mark_account_frozen(account["id"], phone)
        logger.warning(f"[{phone}] 账号任务 {task_type} 执行失败 (task_id={task_id}): {err}")
        await database.finish_account_task(task_id, "failed", err[:490])


async def _do_nickname(client, phone: str, param: dict):
    nickname = (param.get("nickname") or "").strip()
    if not nickname:
        raise ValueError("昵称为空")
    await client(UpdateProfileRequest(first_name=nickname[:64], last_name=""))
    await database.update_account_nickname(phone, nickname[:128])


async def _do_avatar(client, phone: str, param: dict):
    file_path = param.get("filePath") or ""
    if not file_path:
        raise ValueError("头像路径为空")
    ext = os.path.splitext(file_path)[1] or ".jpg"
    fd, local_path = tempfile.mkstemp(prefix="tg_avatar_", suffix=ext)
    os.close(fd)
    try:
        await _download_from_backend(file_path, local_path)
        await _delete_all_profile_photos(client)
        uploaded = await client.upload_file(local_path)
        await client(UploadProfilePhotoRequest(file=uploaded))
    finally:
        try:
            os.remove(local_path)
        except OSError:
            pass


async def _delete_all_profile_photos(client):
    while True:
        result = await client(GetUserPhotosRequest(
            user_id="me", offset=0, max_id=0, limit=100
        ))
        photos = [p for p in result.photos if not isinstance(p, PhotoEmpty)]
        if not photos:
            return
        await client(DeletePhotosRequest(id=[
            InputPhoto(id=p.id, access_hash=p.access_hash,
                       file_reference=p.file_reference)
            for p in photos
        ]))
        if len(result.photos) < 100:
            return


async def _do_twofa(client, phone: str, account: dict, param: dict):
    old_password = param.get("oldPassword") or ""
    new_password = param.get("newPassword") or ""
    if not old_password:
        raise ValueError("缺少旧 2FA 密码")
    if not new_password:
        raise ValueError("缺少新 2FA 密码")
    await client.edit_2fa(current_password=old_password, new_password=new_password)
    await database.update_account_twofa_password(phone, new_password)


async def _download_from_backend(file_path: str, local_path: str):
    url = f"{BACKEND_BASE_URL}{file_path}"
    async with httpx.AsyncClient(timeout=60) as http_client:
        resp = await http_client.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"下载头像失败 status={resp.status_code} {file_path}")
        with open(local_path, "wb") as f:
            f.write(resp.content)
