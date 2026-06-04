#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mysql.connector
from playwright.sync_api import BrowserContext, Page, sync_playwright


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PROFILE_ROOT = Path("/private/tmp/youbi-open-from-db")


@dataclass(frozen=True)
class PlatformConfig:
    key: str
    label: str
    table: str
    state_column: str
    default_url: str
    user_expr: str
    name_expr: str


PLATFORMS: dict[str, PlatformConfig] = {
    "bilibili": PlatformConfig(
        key="bilibili",
        label="Bilibili",
        table="uploader_account_bilibili",
        state_column="playwright_storage_state_json",
        default_url="https://member.bilibili.com/platform/upload/video/frame?page_from=creative_home_top_upload",
        user_expr="COALESCE(CAST(account.playwright_mid AS CHAR), CAST(account.mid AS CHAR), '')",
        name_expr="COALESCE(NULLIF(account.playwright_uname, ''), NULLIF(account.uname, ''), NULLIF(account.display_name, ''), '')",
    ),
    "douyin": PlatformConfig(
        key="douyin",
        label="Douyin",
        table="uploader_account_douyin",
        state_column="storage_state_json",
        default_url="https://creator.douyin.com/creator-micro/content/upload",
        user_expr="COALESCE(account.user_id, '')",
        name_expr="COALESCE(NULLIF(account.nickname, ''), NULLIF(account.display_name, ''), '')",
    ),
    "jinritoutiao": PlatformConfig(
        key="jinritoutiao",
        label="Jinri Toutiao",
        table="uploader_account_jinritoutiao",
        state_column="storage_state_json",
        default_url="https://mp.toutiao.com/",
        user_expr="COALESCE(account.user_id, '')",
        name_expr="COALESCE(NULLIF(account.nickname, ''), NULLIF(account.display_name, ''), '')",
    ),
    "kuaishou": PlatformConfig(
        key="kuaishou",
        label="Kuaishou",
        table="uploader_account_kuaishou",
        state_column="storage_state_json",
        default_url="https://cp.kuaishou.com/article/publish/video",
        user_expr="COALESCE(account.user_id, '')",
        name_expr="COALESCE(NULLIF(account.nickname, ''), NULLIF(account.display_name, ''), '')",
    ),
    "shipinhao": PlatformConfig(
        key="shipinhao",
        label="Shipinhao",
        table="uploader_account_shipinhao",
        state_column="storage_state_json",
        default_url="https://channels.weixin.qq.com/platform",
        user_expr="COALESCE(account.user_id, '')",
        name_expr="COALESCE(NULLIF(account.nickname, ''), NULLIF(account.display_name, ''), '')",
    ),
    "xiaohongshu": PlatformConfig(
        key="xiaohongshu",
        label="Xiaohongshu",
        table="uploader_account_xiaohongshu",
        state_column="storage_state_json",
        default_url="https://creator.xiaohongshu.com/publish/publish?from=homepage&target=video",
        user_expr="COALESCE(account.user_id, '')",
        name_expr="COALESCE(NULLIF(account.nickname, ''), NULLIF(account.display_name, ''), '')",
    ),
}


@dataclass(frozen=True)
class Account:
    account_key: str
    user_id: str
    nickname: str
    is_enabled: bool | None
    storage_state: dict[str, Any]


@dataclass
class OpenedAccount:
    account: Account
    profile_dir: Path
    context: BrowserContext
    page: Page


def parse_args(default_platform: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open platform accounts in isolated temporary Chrome profiles using storage state from MySQL."
    )
    parser.add_argument("--platform", default=default_platform, choices=sorted(PLATFORMS))
    parser.add_argument("--account-key", action="append", default=[], help="Only open selected account_key. Can be specified multiple times.")
    parser.add_argument("--enabled-only", action="store_true", help="Only open accounts marked enabled in uploader_account.")
    parser.add_argument("--url", default="")
    parser.add_argument("--hold-seconds", type=int, default=int(os.getenv("YDBI_OPEN_FROM_DB_HOLD_SECONDS", "0")), help="Seconds to keep Chrome open. 0 means wait until all browser windows are closed.")
    parser.add_argument("--mysql-host", default=os.getenv("YDBI_MYSQL_HOST", DEFAULT_MYSQL_HOST))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("YDBI_MYSQL_PORT", str(DEFAULT_MYSQL_PORT))))
    parser.add_argument("--mysql-database", default=os.getenv("YDBI_MYSQL_DATABASE", DEFAULT_MYSQL_DATABASE))
    parser.add_argument("--mysql-user", default=os.getenv("YDBI_MYSQL_USER", DEFAULT_MYSQL_USER))
    parser.add_argument("--mysql-password", default=os.getenv("YDBI_MYSQL_PASSWORD", DEFAULT_MYSQL_PASSWORD))
    return parser.parse_args()


def connect_mysql(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        connection_timeout=10,
    )


def fetch_accounts(args: argparse.Namespace, config: PlatformConfig) -> list[Account]:
    selected = {value.strip() for value in args.account_key if value.strip()}
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT account.account_key,
                   {config.user_expr} AS user_id,
                   {config.name_expr} AS nickname,
                   state.is_enabled,
                   account.{config.state_column} AS storage_state_json
            FROM {config.table} account
            LEFT JOIN uploader_account state
              ON state.platform = %s
             AND state.account_key = account.account_key
            ORDER BY account.account_key
            """,
            (config.key,),
        )
        accounts = []
        for row in cursor.fetchall():
            account_key = str(row["account_key"])
            if selected and account_key not in selected:
                continue
            if args.enabled_only and not row.get("is_enabled"):
                continue
            storage_state_json = str(row.get("storage_state_json") or "")
            if not storage_state_json:
                print(f"Skip account_key={account_key}: empty storage_state_json", flush=True)
                continue
            accounts.append(
                Account(
                    account_key=account_key,
                    user_id=str(row.get("user_id") or ""),
                    nickname=str(row.get("nickname") or ""),
                    is_enabled=None if row.get("is_enabled") is None else bool(row.get("is_enabled")),
                    storage_state=json.loads(storage_state_json),
                )
            )
        return accounts
    finally:
        connection.close()


def hydrate_storage_state(context: BrowserContext, page: Page, state: dict[str, Any]) -> None:
    cookies = state.get("cookies", [])
    if cookies:
        context.add_cookies(cookies)

    for origin in state.get("origins", []):
        origin_url = str(origin.get("origin") or "")
        local_storage = origin.get("localStorage", [])
        if not origin_url or not local_storage:
            continue
        try:
            page.goto(origin_url, wait_until="domcontentloaded", timeout=30_000)
            page.evaluate(
                """items => {
                    for (const item of items) {
                        window.localStorage.setItem(String(item.name), String(item.value));
                    }
                }""",
                local_storage,
            )
        except Exception as exc:
            print(f"Warn: failed to hydrate localStorage for origin={origin_url}: {exc}", flush=True)


def open_account(playwright, args: argparse.Namespace, config: PlatformConfig, account: Account) -> OpenedAccount:
    profile_root = PROFILE_ROOT / config.key
    profile_root.mkdir(parents=True, exist_ok=True)
    profile_dir = Path(tempfile.mkdtemp(prefix=f"{account.account_key}-", dir=str(profile_root)))
    context = playwright.chromium.launch_persistent_context(
        str(profile_dir),
        executable_path=CHROME_BIN,
        headless=False,
        viewport={"width": 1400, "height": 1000},
        args=["--no-first-run", "--no-default-browser-check"],
    )
    page = context.pages[0] if context.pages else context.new_page()
    hydrate_storage_state(context, page, account.storage_state)
    page.goto(args.url or config.default_url, wait_until="domcontentloaded", timeout=60_000)
    return OpenedAccount(account=account, profile_dir=profile_dir, context=context, page=page)


def context_has_open_pages(context: BrowserContext) -> bool:
    return any(not page.is_closed() for page in context.pages)


def main(default_platform: str) -> int:
    args = parse_args(default_platform)
    config = PLATFORMS[args.platform]
    accounts = fetch_accounts(args, config)
    if not accounts:
        qualifier = "enabled " if args.enabled_only else ""
        raise SystemExit(f"No {qualifier}{config.label} accounts with storage state found.")

    print(f"将打开以下 {config.label} 账号，每个账号使用独立临时 Chrome profile：")
    for account in accounts:
        enabled = "-" if account.is_enabled is None else str(int(account.is_enabled))
        print(f"- {account.account_key}: enabled={enabled} user_id={account.user_id or '-'} nickname={account.nickname or '-'}")
    print()

    opened: list[OpenedAccount] = []
    with sync_playwright() as playwright:
        try:
            for account in accounts:
                opened_account = open_account(playwright, args, config, account)
                opened.append(opened_account)
                state = account.storage_state
                print(
                    f"opened platform={config.key} account_key={account.account_key} "
                    f"cookies={len(state.get('cookies', []))} origins={len(state.get('origins', []))} "
                    f"profile={opened_account.profile_dir} url={opened_account.page.url}",
                    flush=True,
                )

            deadline = time.time() + args.hold_seconds if args.hold_seconds > 0 else None
            while opened and (deadline is None or time.time() < deadline):
                if not any(context_has_open_pages(item.context) for item in opened):
                    break
                time.sleep(1)
        finally:
            for item in opened:
                try:
                    item.context.close()
                except Exception:
                    pass
            for item in opened:
                shutil.rmtree(item.profile_dir, ignore_errors=True)
    return 0
