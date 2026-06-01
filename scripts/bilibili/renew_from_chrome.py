#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import mysql.connector


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
DEFAULT_CDP_URL = "http://127.0.0.1:9222"
BILIBILI_PUBLISH_URL = "https://member.bilibili.com/platform/upload/video/frame?page_from=creative_home_top_upload"
BILIBILI_MYINFO_URL = "https://api.bilibili.com/x/space/myinfo"
BILIBILI_DOMAINS = ("bilibili.com", "hdslb.com")


@dataclass(frozen=True)
class AccountRow:
    account_key: str
    mid: int | None
    uname: str | None
    playwright_mid: int | None
    playwright_uname: str | None


@dataclass(frozen=True)
class Profile:
    mid: int
    uname: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Bilibili Playwright login state from local Chrome CDP into uploader_account_bilibili."
    )
    parser.add_argument("--account-key", default=os.getenv("YDBI_BILIBILI_ACCOUNT_KEY", ""), help="Force one account_key when exactly one login state is found.")
    parser.add_argument("--cdp-url", default=os.getenv("YDBI_CHROME_CDP_URL", DEFAULT_CDP_URL))
    parser.add_argument("--keep-all-origins", action="store_true", help="Store the full Chrome context state instead of only Bilibili domains.")
    parser.add_argument("--dry-run", action="store_true", help="Detect and match accounts without writing MySQL.")
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
            "Quit Chrome, then start it with remote debugging enabled, for example:\n"
            "  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222\n"
            "After Chrome opens, log in to Bilibili in each Chrome profile and run this script again."
        ) from exc
    return playwright, browser


def is_bilibili_host(value: str) -> bool:
    normalized = value.lower().lstrip(".")
    return any(normalized == domain or normalized.endswith("." + domain) for domain in BILIBILI_DOMAINS)


def filter_bilibili_state(state: dict[str, Any]) -> dict[str, Any]:
    cookies = [
        cookie
        for cookie in state.get("cookies", [])
        if is_bilibili_host(str(cookie.get("domain", "")))
    ]
    origins = [
        origin
        for origin in state.get("origins", [])
        if any(domain in str(origin.get("origin", "")).lower() for domain in BILIBILI_DOMAINS)
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
                page.goto(BILIBILI_PUBLISH_URL, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(2_000)
                state = context.storage_state()
            finally:
                page.close()
            state = state if keep_all_origins else filter_bilibili_state(state)
            state["_youbi_context_index"] = context_index
            states.append(state)
        return states
    finally:
        playwright.stop()


def cookie_header(state: dict[str, Any]) -> str:
    pairs = []
    for cookie in state.get("cookies", []):
        domain = str(cookie.get("domain") or "")
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if is_bilibili_host(domain) and name and value:
            pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def has_required_login_cookies(state: dict[str, Any]) -> bool:
    names = {
        str(cookie.get("name") or "")
        for cookie in state.get("cookies", [])
        if is_bilibili_host(str(cookie.get("domain") or "")) and str(cookie.get("value") or "")
    }
    return "DedeUserID" in names and "SESSDATA" in names


def profile_from_storage_state(state: dict[str, Any]) -> Profile | None:
    cookies = cookie_header(state)
    if not cookies:
        return None
    request = Request(
        BILIBILI_MYINFO_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com/",
            "Cookie": cookies,
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8")
    except Exception:
        return None
    try:
        root = json.loads(raw)
    except Exception:
        return None
    if int(root.get("code", -1)) != 0:
        return None
    data = root.get("data") or {}
    mid = data.get("mid")
    uname = str(data.get("name") or "").strip()
    if not isinstance(mid, int) or mid <= 0:
        return None
    return Profile(mid=mid, uname=uname)


def connect_mysql(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        connection_timeout=10,
    )


def load_accounts(args: argparse.Namespace) -> list[AccountRow]:
    connection = connect_mysql(args)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT account_key, mid, uname, playwright_mid, playwright_uname
            FROM uploader_account_bilibili
            ORDER BY account_key
            """
        )
        return [
            AccountRow(
                account_key=str(row["account_key"]),
                mid=int(row["mid"]) if row.get("mid") is not None else None,
                uname=str(row["uname"]) if row.get("uname") is not None else None,
                playwright_mid=int(row["playwright_mid"]) if row.get("playwright_mid") is not None else None,
                playwright_uname=str(row["playwright_uname"]) if row.get("playwright_uname") is not None else None,
            )
            for row in cursor.fetchall()
        ]
    finally:
        connection.close()


def match_account(profile: Profile, accounts: list[AccountRow]) -> AccountRow | None:
    mid_matches = [
        account
        for account in accounts
        if profile.mid in {account.mid, account.playwright_mid}
    ]
    if len(mid_matches) == 1:
        return mid_matches[0]
    if len(mid_matches) > 1:
        raise RuntimeError(f"Ambiguous Bilibili account match for mid={profile.mid}: {[a.account_key for a in mid_matches]}")

    uname_matches = [
        account
        for account in accounts
        if profile.uname and profile.uname in {account.uname, account.playwright_uname}
    ]
    if len(uname_matches) == 1:
        return uname_matches[0]
    if len(uname_matches) > 1:
        raise RuntimeError(f"Ambiguous Bilibili account match for uname={profile.uname}: {[a.account_key for a in uname_matches]}")
    return None


def save_storage_state(args: argparse.Namespace, account_key: str, profile: Profile, storage_state_json: str) -> None:
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
            WHERE account_key = %s
            """,
            (profile.mid, profile.uname, storage_state_json, account_key),
        )
        if cursor.rowcount == 0:
            raise RuntimeError(f"Bilibili account key not found: {account_key}")
        connection.commit()
    finally:
        connection.close()


def main() -> int:
    args = parse_args()
    forced_account_key = normalize_account_key(args.account_key) if str(args.account_key or "").strip() else ""
    accounts = load_accounts(args)
    if not accounts:
        raise SystemExit("No rows found in uploader_account_bilibili.")

    states = load_storage_states(args.cdp_url, args.keep_all_origins)
    candidates = []
    for state in states:
        context_index = int(state.pop("_youbi_context_index", len(candidates) + 1))
        cookie_count = len(state.get("cookies", []))
        origin_count = len(state.get("origins", []))
        if not has_required_login_cookies(state):
            print(f"Skip context={context_index}: missing DedeUserID/SESSDATA cookies cookies={cookie_count} origins={origin_count}")
            continue
        profile = profile_from_storage_state(state)
        if profile is None:
            print(f"Skip context={context_index}: Bilibili myinfo check failed cookies={cookie_count} origins={origin_count}")
            continue
        candidates.append((context_index, state, profile))

    if not candidates:
        raise SystemExit("No valid Bilibili login state found. Open Bilibili in each Chrome profile and log in first.")
    if forced_account_key and len(candidates) != 1:
        raise SystemExit("--account-key can only be used when exactly one valid Bilibili login state is found.")

    updated = 0
    seen_mids: set[int] = set()
    for context_index, state, profile in candidates:
        if profile.mid in seen_mids:
            print(f"Skip context={context_index}: duplicate detected mid={profile.mid} uname={profile.uname}")
            continue
        seen_mids.add(profile.mid)
        matched = None
        if forced_account_key:
            matched = next((account for account in accounts if account.account_key == forced_account_key), None)
            if matched is None:
                raise SystemExit(f"Forced account key not found in uploader_account_bilibili: {forced_account_key}")
        else:
            matched = match_account(profile, accounts)
        if matched is None:
            print(f"Skip context={context_index}: no DB account match for mid={profile.mid} uname={profile.uname}")
            continue

        storage_state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
        byte_size = len(storage_state_json.encode("utf-8"))
        if args.dry_run:
            print(
                "Would update uploader_account_bilibili "
                f"account_key={matched.account_key} context={context_index} mid={profile.mid} uname={profile.uname} "
                f"cookies={len(state.get('cookies', []))} origins={len(state.get('origins', []))} bytes={byte_size}"
            )
        else:
            save_storage_state(args, matched.account_key, profile, storage_state_json)
            updated += 1
            print(
                "Updated uploader_account_bilibili "
                f"account_key={matched.account_key} context={context_index} mid={profile.mid} uname={profile.uname} "
                f"cookies={len(state.get('cookies', []))} origins={len(state.get('origins', []))} bytes={byte_size}"
            )

    if args.dry_run:
        print("Dry run complete.")
    else:
        print(f"Done. updated={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
