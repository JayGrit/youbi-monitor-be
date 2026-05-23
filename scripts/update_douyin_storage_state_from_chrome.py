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
DOUYIN_PUBLISH_URL = "https://creator.douyin.com/creator-micro/content/upload"
DOUYIN_DOMAINS = (
    "douyin.com",
    "douyincdn.com",
    "douyinpic.com",
    "douyinstatic.com",
    "iesdouyin.com",
    "snssdk.com",
    "bytedance.com",
    "bytedance.net",
    "pstatp.com",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Douyin login state from a local Chrome CDP session into yd_douyin_account."
    )
    parser.add_argument("--account-key", default=os.getenv("YDBI_DOUYIN_ACCOUNT_KEY", "douyin_main"))
    parser.add_argument("--cdp-url", default=os.getenv("YDBI_CHROME_CDP_URL", DEFAULT_CDP_URL))
    parser.add_argument("--keep-all-origins", action="store_true", help="Store the full Chrome context state instead of only Douyin domains.")
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


def account_key_for_index(base_key: str, index: int) -> str:
    if index == 1:
        return base_key
    suffix = f"-{index}"
    return f"{base_key[:64 - len(suffix)]}{suffix}"


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
            "Quit Chrome, then start it with remote debugging enabled, for example:\n"
            "  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222\n"
            "After Chrome opens, log in to https://creator.douyin.com/ and run this script again."
        ) from exc
    return playwright, browser


def is_douyin_host(value: str) -> bool:
    normalized = value.lower().lstrip(".")
    return any(normalized == domain or normalized.endswith("." + domain) for domain in DOUYIN_DOMAINS)


def filter_douyin_state(state: dict[str, Any]) -> dict[str, Any]:
    cookies = [
        cookie
        for cookie in state.get("cookies", [])
        if is_douyin_host(str(cookie.get("domain", "")))
    ]
    origins = [
        origin
        for origin in state.get("origins", [])
        if any(domain in str(origin.get("origin", "")).lower() for domain in DOUYIN_DOMAINS)
    ]
    return {"cookies": cookies, "origins": origins}


def load_storage_states(cdp_url: str, keep_all_origins: bool) -> list[dict[str, Any]]:
    playwright, browser = connect_chrome(cdp_url)
    try:
        contexts = list(browser.contexts) or [browser.new_context()]
        states = []
        for context_index, context in enumerate(contexts, start=1):
            page = context.new_page()
            try:
                page.goto(DOUYIN_PUBLISH_URL, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(2_000)
                state = context.storage_state()
            finally:
                page.close()
            state = state if keep_all_origins else filter_douyin_state(state)
            state["_youbi_context_index"] = context_index
            states.append(state)
        return states
    finally:
        playwright.stop()


def first_text(*values: str) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def profile_from_storage_state(state: dict[str, Any]) -> tuple[str | None, str | None]:
    user_id = ""
    nickname = ""
    for cookie in state.get("cookies", []):
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if name in {"passport_assist_user", "sso_uid_tt", "uid_tt"}:
            user_id = first_text(user_id, value)
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            name = str(item.get("name") or "")
            value = str(item.get("value") or "")
            if ("user" not in name.lower() and "nickname" not in value) or not value:
                continue
            try:
                parsed = json.loads(value)
            except Exception:
                continue
            if isinstance(parsed, dict):
                user_id = first_text(user_id, parsed.get("userId"), parsed.get("user_id"), parsed.get("uid"), parsed.get("id"))
                nickname = first_text(nickname, parsed.get("nickname"), parsed.get("name"))
    return (user_id or None, nickname or None)


def ensure_schema(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS yd_douyin_account (
          account_key VARCHAR(64) NOT NULL PRIMARY KEY,
          user_id VARCHAR(128) NULL,
          nickname VARCHAR(128) NULL,
          storage_state_json MEDIUMTEXT NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def save_storage_state(args: argparse.Namespace, account_key: str, storage_state_json: str, user_id: str | None, nickname: str | None) -> None:
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
            INSERT INTO yd_douyin_account (account_key, user_id, nickname, storage_state_json, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
              user_id = VALUES(user_id),
              nickname = VALUES(nickname),
              storage_state_json = VALUES(storage_state_json),
              updated_at = NOW()
            """,
            (account_key, user_id, nickname, storage_state_json),
        )
        connection.commit()
    finally:
        connection.close()


def main() -> int:
    args = parse_args()
    args.account_key = normalize_account_key(args.account_key)
    states = load_storage_states(args.cdp_url, args.keep_all_origins)
    non_empty_states = [
        state
        for state in states
        if len(state.get("cookies", [])) > 0 or len(state.get("origins", [])) > 0
    ]
    if not non_empty_states:
        raise SystemExit("No Douyin cookies or localStorage found in Chrome. Open creator.douyin.com and log in first.")

    for output_index, state in enumerate(non_empty_states, start=1):
        context_index = int(state.pop("_youbi_context_index", output_index))
        cookie_count = len(state.get("cookies", []))
        origin_count = len(state.get("origins", []))
        user_id, nickname = profile_from_storage_state(state)
        storage_state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
        account_key = account_key_for_index(args.account_key, output_index)
        save_storage_state(args, account_key, storage_state_json, user_id, nickname)
        print(
            "Updated yd_douyin_account "
            f"account_key={account_key} context={context_index} cookies={cookie_count} origins={origin_count} "
            f"bytes={len(storage_state_json.encode('utf-8'))}"
        )
        if nickname:
            print(f"Detected nickname={nickname}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
