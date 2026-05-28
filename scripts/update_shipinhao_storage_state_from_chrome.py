#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any
from urllib.parse import urlparse

import mysql.connector


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
DEFAULT_CDP_URL = "http://127.0.0.1:9222"
SHIPINHAO_PLATFORM_URL = "https://channels.weixin.qq.com/platform"
SHIPINHAO_DOMAINS = (
    "channels.weixin.qq.com",
    "weixin.qq.com",
    "wx.qq.com",
    "qq.com",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Shipinhao login state from a local Chrome CDP session into uploader_account_shipinhao."
    )
    parser.add_argument("--account-key", default=os.getenv("YDBI_SHIPINHAO_ACCOUNT_KEY", "default"))
    parser.add_argument("--cdp-url", default=os.getenv("YDBI_CHROME_CDP_URL", DEFAULT_CDP_URL))
    parser.add_argument("--keep-all-origins", action="store_true", help="Store the full Chrome context state instead of only Weixin domains.")
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


def import_playwright():
    try:
        from playwright.sync_api import Error, sync_playwright
    except ImportError as exc:
        raise SystemExit(
            "Missing Python package: playwright\n"
            "Install it in this repo venv first, for example:\n"
            "  .venv/bin/pip install playwright\n"
        ) from exc
    return Error, sync_playwright


def disable_proxy_for_local_cdp(cdp_url: str) -> None:
    host = (urlparse(cdp_url).hostname or "").lower()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(name, None)
    no_proxy = "127.0.0.1,localhost,::1"
    os.environ["NO_PROXY"] = no_proxy
    os.environ["no_proxy"] = no_proxy


def connect_chrome(cdp_url: str):
    disable_proxy_for_local_cdp(cdp_url)
    playwright_error, sync_playwright = import_playwright()
    playwright = sync_playwright().start()
    try:
        browser = playwright.chromium.connect_over_cdp(cdp_url)
    except playwright_error as exc:
        playwright.stop()
        raise SystemExit(
            f"Cannot connect to Chrome DevTools at {cdp_url}.\n"
            f"Playwright error: {exc}\n"
            "Start Chrome with remote debugging enabled and the Default profile, for example:\n"
            "  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome "
            "--remote-debugging-port=9222 --profile-directory=Default\n"
            f"Then open {SHIPINHAO_PLATFORM_URL}, confirm it is logged in, and run this script again."
        ) from exc
    return playwright, browser


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


def load_storage_state(cdp_url: str, keep_all_origins: bool) -> tuple[dict[str, Any], str]:
    playwright, browser = connect_chrome(cdp_url)
    try:
        contexts = list(browser.contexts) or [browser.new_context()]
        context = contexts[0]
        page = context.new_page()
        try:
            page.goto(SHIPINHAO_PLATFORM_URL, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(5_000)
            url = page.url
            state = context.storage_state()
        finally:
            page.close()
        return (state if keep_all_origins else filter_shipinhao_state(state), url)
    finally:
        playwright.stop()


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


def main() -> int:
    args = parse_args()
    args.account_key = normalize_account_key(args.account_key)
    state, final_url = load_storage_state(args.cdp_url, args.keep_all_origins)
    cookie_count = len(state.get("cookies", []))
    origin_count = len(state.get("origins", []))
    if cookie_count == 0 and origin_count == 0:
        raise SystemExit(f"No Shipinhao cookies or localStorage found after opening {final_url}.")

    user_id, nickname = profile_from_storage_state(state)
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
