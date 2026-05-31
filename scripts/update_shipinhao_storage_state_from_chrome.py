#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
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
SHIPINHAO_PLATFORM_URL = "https://channels.weixin.qq.com/platform"
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PROFILE_ROOT = Path("/private/tmp/youbi-shipinhao-manual-login")
SHIPINHAO_DOMAINS = (
    "channels.weixin.qq.com",
    "weixin.qq.com",
    "wx.qq.com",
    "qq.com",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open a temporary Chrome profile, wait for Shipinhao scan login, then save storage state into uploader_account_shipinhao."
    )
    parser.add_argument("--account-key", default=os.getenv("YDBI_SHIPINHAO_ACCOUNT_KEY", "default"))
    parser.add_argument("--keep-all-origins", action="store_true", help="Store the full Chrome context state instead of only Weixin domains.")
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("YDBI_SHIPINHAO_LOGIN_TIMEOUT_SECONDS", "600")))
    parser.add_argument("--fresh", action="store_true", help="Delete the temporary Chrome profile dir before login.")
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


def is_shipinhao_host(value: str) -> bool:
    normalized = value.lower().lstrip(".")
    return any(normalized == domain or normalized.endswith("." + domain) for domain in SHIPINHAO_DOMAINS)


def filter_shipinhao_state(state: dict[str, Any]) -> dict[str, Any]:
    cookies = [
        cookie
        for cookie in state.get("cookies", [])
        if is_shipinhao_host(str(cookie.get("domain", "")))
    ]
    origins = [
        origin
        for origin in state.get("origins", [])
        if any(domain in str(origin.get("origin", "")).lower() for domain in SHIPINHAO_DOMAINS)
    ]
    return {"cookies": cookies, "origins": origins}


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def profile_from_storage_state(state: dict[str, Any]) -> tuple[str | None, str | None]:
    user_id = ""
    nickname = ""
    for cookie in state.get("cookies", []):
        name = str(cookie.get("name") or "").lower()
        value = str(cookie.get("value") or "")
        if name in {"wxuin", "uin", "finderuin", "finder_uin"}:
            user_id = first_text(user_id, value)
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            name = str(item.get("name") or "")
            value = str(item.get("value") or "")
            lower = f"{name} {value}".lower()
            if not any(marker in lower for marker in ("user", "finder", "nickname", "nick_name")):
                continue
            try:
                parsed = json.loads(value)
            except Exception:
                continue
            if isinstance(parsed, dict):
                user_id = first_text(user_id, parsed.get("userId"), parsed.get("user_id"), parsed.get("uin"), parsed.get("id"))
                nickname = first_text(nickname, parsed.get("nickname"), parsed.get("nickName"), parsed.get("nick_name"), parsed.get("name"))
    return (user_id or None, nickname or None)


def has_login_cookie(state: dict[str, Any]) -> bool:
    for cookie in state.get("cookies", []):
        name = str(cookie.get("name") or "").lower()
        value = str(cookie.get("value") or "").strip()
        if name in {"wxuin", "uin", "finderuin", "finder_uin"} and value and value != "0":
            return True
    return False


def page_login_hint(page) -> bool:
    try:
        body_text = page.locator("body").inner_text(timeout=1_000)
    except Exception:
        return False
    positive_markers = (
        "发表视频",
        "发布视频",
        "视频管理",
        "数据概览",
        "创作者中心",
        "助手",
        "账号设置",
    )
    negative_markers = ("扫码登录", "微信扫码", "请使用微信")
    return any(marker in body_text for marker in positive_markers) and not any(marker in body_text for marker in negative_markers)


def looks_logged_in(state: dict[str, Any], page, user_id: str | None, nickname: str | None) -> bool:
    return bool(user_id or nickname or has_login_cookie(state) or page_login_hint(page))


def ensure_schema(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS uploader_account_shipinhao (
          account_key VARCHAR(64) NOT NULL PRIMARY KEY,
          user_id VARCHAR(128) NULL,
          nickname VARCHAR(128) NULL,
          storage_state_json MEDIUMTEXT NOT NULL,
          is_enabled TINYINT(1) NOT NULL DEFAULT 1,
          upload_cooldown_min_seconds INT NOT NULL DEFAULT 3600,
          upload_cooldown_max_seconds INT NOT NULL DEFAULT 7200,
          last_upload_at DATETIME NULL,
          next_upload_allowed_at DATETIME NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def save_storage_state(args: argparse.Namespace, storage_state_json: str, user_id: str | None, nickname: str | None) -> None:
    connection = mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        connection_timeout=10,
    )
    try:
        cursor = connection.cursor()
        ensure_schema(cursor)
        cursor.execute(
            """
            INSERT INTO uploader_account_shipinhao (account_key, user_id, nickname, storage_state_json, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
              user_id = VALUES(user_id),
              nickname = VALUES(nickname),
              storage_state_json = VALUES(storage_state_json),
              updated_at = NOW()
            """,
            (args.account_key, user_id, nickname, storage_state_json),
        )
        connection.commit()
    finally:
        connection.close()


def wait_for_login(args: argparse.Namespace) -> tuple[dict[str, Any], str, str | None, str | None]:
    profile_dir = PROFILE_ROOT / args.account_key
    if args.fresh and profile_dir.exists():
        shutil.rmtree(profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)

    print()
    print("=" * 72)
    print(f"请在弹出的 Chrome 窗口扫码登录视频号：account_key={args.account_key}")
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
            page.goto(SHIPINHAO_PLATFORM_URL, wait_until="domcontentloaded", timeout=60_000)
            while time.time() < deadline:
                state = context.storage_state()
                if not args.keep_all_origins:
                    state = filter_shipinhao_state(state)
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
                if cookie_count > 0 and looks_logged_in(state, page, user_id, nickname):
                    return state, page.url, user_id, nickname
                page.wait_for_timeout(3_000)
            raise SystemExit(f"等待视频号扫码登录超时：account_key={args.account_key}")
        finally:
            context.close()


def main() -> int:
    args = parse_args()
    args.account_key = normalize_account_key(args.account_key)
    state, final_url, user_id, nickname = wait_for_login(args)
    cookie_count = len(state.get("cookies", []))
    origin_count = len(state.get("origins", []))
    if cookie_count == 0 and origin_count == 0:
        raise SystemExit(f"No Shipinhao cookies or localStorage found after opening {final_url}.")

    storage_state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    save_storage_state(args, storage_state_json, user_id, nickname)
    print(
        "Updated uploader_account_shipinhao "
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
