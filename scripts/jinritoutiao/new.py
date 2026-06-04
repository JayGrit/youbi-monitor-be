#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import mysql.connector
from playwright.sync_api import sync_playwright


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
JINRITOUTIAO_CREATOR_URL = "https://mp.toutiao.com/"
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PROFILE_ROOT = Path("/private/tmp/youbi-jinritoutiao-manual-login")
JINRITOUTIAO_DOMAINS = (
    "toutiao.com",
    "toutiaocdn.com",
    "toutiaoimg.com",
    "toutiaostatic.com",
    "byteimg.com",
    "bytedance.com",
    "bytedance.net",
    "snssdk.com",
    "pstatp.com",
    "ibytedtos.com",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open a fresh temporary Chrome profile, wait for Jinri Toutiao scan login, then insert a new uploader_account_jinritoutiao row."
    )
    parser.add_argument("--account-key", default=os.getenv("YDBI_JINRITOUTIAO_ACCOUNT_KEY", "default"), help="Base account key. If it already exists, the script uses base-1, base-2, ...")
    parser.add_argument("--keep-all-origins", action="store_true", help="Store the full Chrome context state instead of only Jinri Toutiao domains.")
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("YDBI_JINRITOUTIAO_LOGIN_TIMEOUT_SECONDS", "600")))
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


def account_key_candidate(base_key: str, index: int) -> str:
    if index == 0:
        return base_key
    suffix = f"-{index}"
    return f"{base_key[:64 - len(suffix)]}{suffix}"


def next_account_key(args: argparse.Namespace, base_key: str) -> str:
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor()
        ensure_schema(cursor)
        cursor.execute("SELECT account_key FROM uploader_account_jinritoutiao")
        existing = {str(row[0]) for row in cursor.fetchall()}
    finally:
        connection.close()

    for index in range(0, 1000):
        candidate = account_key_candidate(base_key, index)
        if candidate not in existing:
            return candidate
    raise RuntimeError(f"No available Jinri Toutiao account key for base: {base_key}")


def is_jinritoutiao_host(value: str) -> bool:
    normalized = value.lower().lstrip(".")
    return any(normalized == domain or normalized.endswith("." + domain) for domain in JINRITOUTIAO_DOMAINS)


def filter_jinritoutiao_state(state: dict[str, Any]) -> dict[str, Any]:
    cookies = [
        cookie
        for cookie in state.get("cookies", [])
        if is_jinritoutiao_host(str(cookie.get("domain", "")))
    ]
    origins = [
        origin
        for origin in state.get("origins", [])
        if any(domain in str(origin.get("origin", "")).lower() for domain in JINRITOUTIAO_DOMAINS)
    ]
    return {"cookies": cookies, "origins": origins}


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def profile_values_from_json(value: Any) -> tuple[str, str]:
    user_id = ""
    nickname = ""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            user_id = first_text(
                user_id,
                item.get("userId"),
                item.get("user_id"),
                item.get("uid"),
                item.get("id") if any(key in item for key in ("nickname", "name", "screen_name", "userName")) else "",
            )
            nickname = first_text(
                nickname,
                item.get("nickname"),
                item.get("nickName"),
                item.get("screen_name"),
                item.get("userName"),
                item.get("name"),
            )
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return user_id, nickname


def profile_from_storage_state(state: dict[str, Any]) -> tuple[str | None, str | None]:
    user_id = ""
    nickname = ""
    for cookie in state.get("cookies", []):
        name = str(cookie.get("name") or "").lower()
        value = str(cookie.get("value") or "")
        if name in {"sso_uid_tt", "uid_tt", "passport_assist_user", "user_id", "uid"}:
            user_id = first_text(user_id, value)
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            name = str(item.get("name") or "")
            value = str(item.get("value") or "")
            lower = f"{name} {value}".lower()
            if not any(marker in lower for marker in ("user", "profile", "account", "nickname", "screen_name", "username")):
                continue
            try:
                parsed = json.loads(value)
            except Exception:
                continue
            parsed_user_id, parsed_nickname = profile_values_from_json(parsed)
            user_id = first_text(user_id, parsed_user_id)
            nickname = first_text(nickname, parsed_nickname)
    return (user_id or None, nickname or None)


def has_login_cookie(state: dict[str, Any]) -> bool:
    for cookie in state.get("cookies", []):
        name = str(cookie.get("name") or "").lower()
        value = str(cookie.get("value") or "").strip()
        if not value:
            continue
        if name in {"sessionid", "sid_tt", "passport_auth_status"}:
            return True
    return False


def is_login_page(page) -> bool:
    current_url = page.url.lower()
    if any(marker in current_url for marker in ("/login", "login", "bind_mobile", "/register/")):
        return True
    try:
        body_text = page.locator("body").inner_text(timeout=1_000)
    except Exception:
        return False
    negative_markers = ("扫码登录", "验证码登录", "手机登录", "密码登录", "抖音扫码登录")
    return any(marker in body_text for marker in negative_markers)


def page_login_hint(page) -> bool:
    try:
        body_text = page.locator("body").inner_text(timeout=1_000)
    except Exception:
        return False
    positive_markers = (
        "头条号",
        "创作者",
        "发布",
        "作品管理",
        "数据",
        "收益",
        "粉丝",
        "账号设置",
    )
    negative_markers = ("扫码登录", "验证码登录", "手机登录", "密码登录")
    return any(marker in body_text for marker in positive_markers) and not any(marker in body_text for marker in negative_markers)


def looks_logged_in(state: dict[str, Any], page, user_id: str | None, nickname: str | None) -> bool:
    if is_login_page(page):
        return False
    return bool(nickname or has_login_cookie(state) or page_login_hint(page))


def ensure_schema(cursor) -> None:
    return

def ensure_surrogate_primary_key(cursor, table: str) -> None:
    return

def save_storage_state(args: argparse.Namespace, storage_state_json: str, user_id: str | None, nickname: str | None) -> None:
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor()
        ensure_schema(cursor)
        cursor.execute(
            """
            INSERT INTO uploader_account_jinritoutiao (account_key, user_id, nickname, storage_state_json, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (args.account_key, user_id, nickname, storage_state_json),
        )
        connection.commit()
    finally:
        connection.close()


def wait_for_login(args: argparse.Namespace) -> tuple[dict[str, Any], str, str | None, str | None]:
    PROFILE_ROOT.mkdir(parents=True, exist_ok=True)
    profile_dir = Path(tempfile.mkdtemp(prefix=f"{args.account_key}-", dir=str(PROFILE_ROOT)))

    print()
    print("=" * 72)
    print(f"请在弹出的 Chrome 窗口扫码登录今日头条账号：account_key={args.account_key}")
    print(f"使用全新临时 Chrome profile：{profile_dir}")
    print("脚本会轮询登录态；识别到登录后自动写入数据库。")
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
            page.goto(JINRITOUTIAO_CREATOR_URL, wait_until="domcontentloaded", timeout=60_000)
            while time.time() < deadline:
                state = context.storage_state()
                if not args.keep_all_origins:
                    state = filter_jinritoutiao_state(state)
                user_id, nickname = profile_from_storage_state(state)
                cookie_count = len(state.get("cookies", []))
                origin_count = len(state.get("origins", []))
                status = (
                    f"cookies={cookie_count} origins={origin_count} "
                    f"has_login_cookie={has_login_cookie(state)} "
                    f"is_login_page={is_login_page(page)} "
                    f"user_id={user_id or '-'} nickname={nickname or '-'} url={page.url}"
                )
                if status != last_status:
                    print(status, flush=True)
                    last_status = status
                if cookie_count > 0 and looks_logged_in(state, page, user_id, nickname):
                    return state, page.url, user_id, nickname
                page.wait_for_timeout(3_000)
            raise SystemExit(f"等待今日头条扫码登录超时：account_key={args.account_key}")
        finally:
            context.close()


def main() -> int:
    args = parse_args()
    base_account_key = normalize_account_key(args.account_key)
    args.account_key = next_account_key(args, base_account_key)
    if args.account_key != base_account_key:
        print(f"account_key={base_account_key} 已存在，本次新增账号将写入 account_key={args.account_key}")
    state, final_url, user_id, nickname = wait_for_login(args)
    cookie_count = len(state.get("cookies", []))
    origin_count = len(state.get("origins", []))
    if cookie_count == 0 and origin_count == 0:
        raise SystemExit(f"No Jinri Toutiao cookies or localStorage found after opening {final_url}.")

    storage_state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    save_storage_state(args, storage_state_json, user_id, nickname)
    print(
        "Updated uploader_account_jinritoutiao "
        f"account_key={args.account_key} cookies={cookie_count} origins={origin_count} "
        f"bytes={len(storage_state_json.encode('utf-8'))} url={final_url}"
    )
    if user_id:
        print(f"Detected user_id={user_id}")
    if nickname:
        print(f"Detected nickname={nickname}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
