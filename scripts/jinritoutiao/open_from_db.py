#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time

import mysql.connector
from playwright.sync_api import sync_playwright


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
JINRITOUTIAO_CREATOR_URL = "https://mp.toutiao.com/"
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open Jinri Toutiao creator center in a temporary Chrome using storage_state_json from MySQL."
    )
    parser.add_argument("--account-key", default=os.getenv("YDBI_JINRITOUTIAO_ACCOUNT_KEY", ""), help="Account key to open. Defaults to the latest enabled account.")
    parser.add_argument("--url", default=os.getenv("YDBI_JINRITOUTIAO_URL", JINRITOUTIAO_CREATOR_URL))
    parser.add_argument("--hold-seconds", type=int, default=int(os.getenv("YDBI_JINRITOUTIAO_HOLD_SECONDS", "0")), help="Seconds to keep Chrome open. 0 means wait until the browser window is closed.")
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


def fetch_storage_state(args: argparse.Namespace) -> tuple[str, str]:
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor()
        if args.account_key:
            cursor.execute(
                """
                SELECT account_key, storage_state_json
                FROM uploader_account_jinritoutiao
                WHERE account_key = %s AND is_enabled = 1
                """,
                (args.account_key,),
            )
        else:
            cursor.execute(
                """
                SELECT account_key, storage_state_json
                FROM uploader_account_jinritoutiao
                WHERE is_enabled = 1
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
        row = cursor.fetchone()
    finally:
        connection.close()

    if not row:
        if args.account_key:
            raise SystemExit(f"No enabled Jinri Toutiao account found for account_key={args.account_key!r}.")
        raise SystemExit("No enabled Jinri Toutiao account found.")
    return str(row[0]), str(row[1])


def main() -> int:
    args = parse_args()
    account_key, storage_state_json = fetch_storage_state(args)
    state = json.loads(storage_state_json)

    print(f"account_key={account_key}")
    print(f"cookies={len(state.get('cookies', []))} origins={len(state.get('origins', []))}")
    print(f"opening={args.url}", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=CHROME_BIN,
            headless=False,
            args=["--no-first-run", "--no-default-browser-check"],
        )
        context = browser.new_context(storage_state=state, viewport={"width": 1400, "height": 1000})
        page = context.new_page()
        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)
        print(f"url={page.url}", flush=True)

        deadline = time.time() + args.hold_seconds if args.hold_seconds > 0 else None
        while browser.is_connected() and (deadline is None or time.time() < deadline):
            page.wait_for_timeout(1_000)
        if browser.is_connected():
            browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
