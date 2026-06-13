#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mysql.connector
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from uploader_new_account_common import mark_uploader_account_available

from new import (
    CHROME_BIN,
    DEFAULT_MYSQL_DATABASE,
    DEFAULT_MYSQL_HOST,
    DEFAULT_MYSQL_PASSWORD,
    DEFAULT_MYSQL_PORT,
    DEFAULT_MYSQL_USER,
    DOUBAO_CREATOR_URL,
    PROFILE_ROOT,
    ensure_schema,
    filter_doubao_state,
    has_login_cookie,
    profile_from_storage_state,
)


@dataclass(frozen=True)
class Account:
    account_key: str
    user_id: str
    nickname: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open one temporary Chrome profile per Doubao account, wait for scan login, verify known profile info, then save storage state to MySQL."
    )
    parser.add_argument("--account-key", action="append", default=[], help="Only update selected account_key. Can be specified multiple times.")
    parser.add_argument("--keep-all-origins", action="store_true", help="Store the full Chrome context state instead of only Doubao domains.")
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("YDBI_DOUBAO_LOGIN_TIMEOUT_SECONDS", "600")))
    parser.add_argument("--fresh", action="store_true", help="Ignored for compatibility; renew always starts from a clean temporary profile.")
    parser.add_argument("--mysql-host", default=os.getenv("YDBI_MYSQL_HOST", DEFAULT_MYSQL_HOST))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("YDBI_MYSQL_PORT", str(DEFAULT_MYSQL_PORT))))
    parser.add_argument("--mysql-database", default=os.getenv("YDBI_MYSQL_DATABASE", DEFAULT_MYSQL_DATABASE))
    parser.add_argument("--mysql-user", default=os.getenv("YDBI_MYSQL_USER", DEFAULT_MYSQL_USER))
    parser.add_argument("--mysql-password", default=os.getenv("YDBI_MYSQL_PASSWORD", DEFAULT_MYSQL_PASSWORD))
    return parser.parse_args()


def normalize_account_key(value: str) -> str:
    account_key = value.strip()
    if not account_key or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", account_key):
        raise ValueError(f"Invalid account key: {value!r}")
    return account_key


def connect_mysql(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        connection_timeout=10,
    )


def load_accounts(args: argparse.Namespace) -> list[Account]:
    selected = {normalize_account_key(value) for value in args.account_key}
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor(dictionary=True)
        ensure_schema(cursor)
        cursor.execute(
            """
            SELECT account.account_key, account.user_id, account.nickname
            FROM publisher_account_doubao account
            LEFT JOIN uploader_account unified
              ON unified.platform = 'doubao'
             AND unified.account_key = account.account_key
            ORDER BY
              CASE WHEN unified.is_available = 1 THEN 1 ELSE 0 END,
              account.account_key
            """
        )
        accounts = []
        for row in cursor.fetchall():
            account_key = str(row["account_key"])
            if selected and account_key not in selected:
                continue
            accounts.append(
                Account(
                    account_key=account_key,
                    user_id=str(row.get("user_id") or ""),
                    nickname=str(row.get("nickname") or ""),
                )
            )
        return accounts
    finally:
        connection.close()


def save_storage_state(args: argparse.Namespace, account: Account, state_json: str, user_id: str | None, nickname: str | None) -> None:
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE publisher_account_doubao
            SET user_id = %s,
                nickname = %s,
                storage_state_json = %s,
                updated_at = NOW()
            WHERE account_key = %s
            """,
            (user_id or account.user_id or None, nickname or account.nickname or None, state_json, account.account_key),
        )
        if cursor.rowcount == 0:
            raise RuntimeError(f"Doubao account key not found: {account.account_key}")
        mark_uploader_account_available(cursor, "doubao", account.account_key, "publisher_account_doubao")
        connection.commit()
    finally:
        connection.close()


def profile_matches(account: Account, user_id: str | None, nickname: str | None) -> bool:
    if account.user_id and user_id and account.user_id != user_id:
        return False
    if account.nickname and nickname and account.nickname != nickname:
        return False
    return True


def has_enough_profile_to_verify(account: Account, user_id: str | None, nickname: str | None) -> bool:
    if account.user_id and user_id == account.user_id:
        return True
    if account.nickname and nickname == account.nickname:
        return True
    return not account.user_id and not account.nickname


def login_ready(state: dict[str, Any], page, user_id: str | None, nickname: str | None) -> bool:
    return bool(user_id or nickname or has_login_cookie(state))


def wait_for_login(args: argparse.Namespace, account: Account) -> bool:
    PROFILE_ROOT.mkdir(parents=True, exist_ok=True)
    profile_dir = Path(tempfile.mkdtemp(prefix=f"{account.account_key}-", dir=str(PROFILE_ROOT)))

    print()
    print("=" * 72)
    print(
        "请在弹出的 Chrome 窗口扫码登录豆包账号："
        f"key={account.account_key} user_id={account.user_id or '-'} nickname={account.nickname or '-'}"
    )
    print("脚本会轮询登录态；已保存的 user_id/nickname 匹配后才写入数据库。")
    print(f"使用全新临时 Chrome profile：{profile_dir}")
    print("=" * 72, flush=True)

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            str(profile_dir),
            executable_path=CHROME_BIN,
            headless=False,
            viewport={"width": 1400, "height": 1000},
            args=["--no-first-run", "--no-default-browser-check"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        deadline = time.time() + args.timeout_seconds
        last_status = ""
        try:
            page.goto(DOUBAO_CREATOR_URL, wait_until="domcontentloaded", timeout=60_000)
            while time.time() < deadline:
                state = context.storage_state()
                if not args.keep_all_origins:
                    state = filter_doubao_state(state)
                user_id, nickname = profile_from_storage_state(state)
                cookie_count = len(state.get("cookies", []))
                origin_count = len(state.get("origins", []))
                status = (
                    f"cookies={cookie_count} origins={origin_count} "
                    f"has_login_cookie={has_login_cookie(state)} "
                    f"user_id={user_id or '-'} nickname={nickname or '-'} url={page.url}"
                )
                if status != last_status:
                    print(status, flush=True)
                    last_status = status
                if cookie_count > 0 and login_ready(state, page, user_id, nickname):
                    if not profile_matches(account, user_id, nickname):
                        print(
                            f"账号不匹配：实际 user_id={user_id or '-'} nickname={nickname or '-'}；"
                            f"期望 user_id={account.user_id or '-'} nickname={account.nickname or '-'}。"
                            "请退出当前账号后重新登录目标账号。",
                            flush=True,
                        )
                    elif not has_enough_profile_to_verify(account, user_id, nickname):
                        print("已看到登录态，但暂未提取到足够的账号信息用于校验，继续等待...", flush=True)
                    else:
                        state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
                        save_storage_state(args, account, state_json, user_id, nickname)
                        print(
                            f"已写入：account_key={account.account_key} user_id={user_id or account.user_id or '-'} "
                            f"nickname={nickname or account.nickname or '-'} bytes={len(state_json.encode('utf-8'))}",
                            flush=True,
                        )
                        return True
                page.wait_for_timeout(3_000)
            print(f"等待超时：account_key={account.account_key}", flush=True)
            return False
        finally:
            context.close()
            shutil.rmtree(profile_dir, ignore_errors=True)


def main() -> int:
    args = parse_args()
    accounts = load_accounts(args)
    if not accounts:
        raise SystemExit("No Doubao accounts found.")

    print("将依次更新以下豆包账号：")
    for account in accounts:
        print(f"- {account.account_key}: user_id={account.user_id or '-'} nickname={account.nickname or '-'}")

    failed = []
    for account in accounts:
        if not wait_for_login(args, account):
            failed.append(account.account_key)

    print()
    if failed:
        print("完成，但以下账号未更新成功：" + ", ".join(failed))
        return 1
    print("全部豆包登录态已更新完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
