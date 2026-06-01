#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

import mysql.connector
from playwright.sync_api import sync_playwright


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
PUBLISH_URL = "https://member.bilibili.com/platform/upload/video/frame?page_from=creative_home_top_upload"
MYINFO_URL = "https://api.bilibili.com/x/space/myinfo"
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PROFILE_ROOT = Path("/private/tmp/youbi-bilibili-manual-login")


@dataclass(frozen=True)
class Account:
    account_key: str
    mid: int
    uname: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open one temporary Chrome profile per Bilibili account, wait for scan login, verify mid, then save Playwright storage state to MySQL."
    )
    parser.add_argument("--account-key", action="append", default=[], help="Only update selected account_key. Can be specified multiple times.")
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("YDBI_BILIBILI_LOGIN_TIMEOUT_SECONDS", "600")))
    parser.add_argument("--fresh", action="store_true", help="Delete temporary Chrome profile dirs before each login.")
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
        cursor.execute(
            """
            SELECT account.account_key, account.mid, account.uname
            FROM uploader_account_bilibili account
            JOIN uploader_account state
              ON state.platform = 'bilibili'
             AND state.account_key = account.account_key
             AND state.is_enabled = 1
            ORDER BY account.account_key
            """
        )
        accounts = []
        for row in cursor.fetchall():
            account_key = str(row["account_key"])
            if selected and account_key not in selected:
                continue
            if row.get("mid") is None:
                print(f"Skip account_key={account_key}: missing mid")
                continue
            accounts.append(Account(account_key, int(row["mid"]), str(row.get("uname") or "")))
        return accounts
    finally:
        connection.close()


def is_bilibili_host(value: str) -> bool:
    normalized = value.lower().lstrip(".")
    return (
        normalized == "bilibili.com"
        or normalized.endswith(".bilibili.com")
        or normalized == "hdslb.com"
        or normalized.endswith(".hdslb.com")
    )


def filter_state(state: dict) -> dict:
    return {
        "cookies": [cookie for cookie in state.get("cookies", []) if is_bilibili_host(str(cookie.get("domain") or ""))],
        "origins": [
            origin
            for origin in state.get("origins", [])
            if "bilibili.com" in str(origin.get("origin") or "").lower()
            or "hdslb.com" in str(origin.get("origin") or "").lower()
        ],
    }


def cookie_names(state: dict) -> set[str]:
    return {
        str(cookie.get("name") or "")
        for cookie in state.get("cookies", [])
        if is_bilibili_host(str(cookie.get("domain") or "")) and str(cookie.get("value") or "")
    }


def cookie_header(state: dict) -> str:
    pairs = []
    for cookie in state.get("cookies", []):
        domain = str(cookie.get("domain") or "")
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if is_bilibili_host(domain) and name and value:
            pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def myinfo(state: dict) -> tuple[int, str] | None:
    request = Request(
        MYINFO_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com/",
            "Cookie": cookie_header(state),
        },
    )
    try:
        with urlopen(request, timeout=15) as response:
            root = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    if int(root.get("code", -1)) != 0:
        return None
    data = root.get("data") or {}
    mid = data.get("mid")
    name = str(data.get("name") or "").strip()
    if not isinstance(mid, int) or mid <= 0:
        return None
    return mid, name


def save_storage_state(args: argparse.Namespace, account: Account, state_json: str, actual_name: str) -> None:
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE uploader_account_bilibili
            SET playwright_mid = %s,
                playwright_uname = %s,
                playwright_storage_state_json = %s,
                playwright_updated_at = NOW()
            WHERE account_key = %s AND mid = %s
            """,
            (account.mid, actual_name, state_json, account.account_key, account.mid),
        )
        if cursor.rowcount == 0:
            raise RuntimeError(f"No matching DB row for account_key={account.account_key} mid={account.mid}")
        connection.commit()
    finally:
        connection.close()


def wait_for_login(args: argparse.Namespace, account: Account) -> bool:
    profile_dir = PROFILE_ROOT / account.account_key
    if args.fresh and profile_dir.exists():
        shutil.rmtree(profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    print()
    print("=" * 72)
    print(f"请在弹出的 Chrome 窗口登录 B 站账号：key={account.account_key} name={account.uname} mid={account.mid}")
    print("脚本会轮询登录态；账号 mid 匹配后才写入数据库。")
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
        page.goto(PUBLISH_URL, wait_until="domcontentloaded", timeout=60000)
        deadline = time.time() + args.timeout_seconds
        last_status = ""
        try:
            while time.time() < deadline:
                state = filter_state(context.storage_state())
                names = cookie_names(state)
                status = (
                    f"cookies={len(state.get('cookies', []))} "
                    f"has_DedeUserID={'DedeUserID' in names} has_SESSDATA={'SESSDATA' in names}"
                )
                if status != last_status:
                    print(status, flush=True)
                    last_status = status
                if "DedeUserID" in names and "SESSDATA" in names:
                    profile = myinfo(state)
                    if profile is None:
                        print("已看到核心 cookie，但 myinfo 校验暂未通过，继续等待...", flush=True)
                    else:
                        actual_mid, actual_name = profile
                        if actual_mid != account.mid:
                            print(
                                f"账号不匹配：实际 mid={actual_mid} name={actual_name}；"
                                f"期望 mid={account.mid} name={account.uname}。请退出当前账号后重新登录目标账号。",
                                flush=True,
                            )
                        else:
                            state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
                            save_storage_state(args, account, state_json, actual_name)
                            print(
                                f"已写入：account_key={account.account_key} mid={actual_mid} "
                                f"name={actual_name} bytes={len(state_json.encode('utf-8'))}",
                                flush=True,
                            )
                            return True
                page.wait_for_timeout(3000)
            print(f"等待超时：account_key={account.account_key}", flush=True)
            return False
        finally:
            context.close()


def main() -> int:
    args = parse_args()
    accounts = load_accounts(args)
    if not accounts:
        raise SystemExit("No enabled Bilibili accounts found.")
    print("将依次更新以下 B 站账号：")
    for account in accounts:
        print(f"- {account.account_key}: {account.uname} ({account.mid})")

    failed = []
    for account in accounts:
        if not wait_for_login(args, account):
            failed.append(account.account_key)

    print()
    if failed:
        print("完成，但以下账号未更新成功：" + ", ".join(failed))
        return 1
    print("全部 Bilibili Playwright 登录态已更新完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
