#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import mysql.connector
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


DEFAULT_MYSQL_HOST = "120.53.92.66"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_DATABASE = "youbi"
DEFAULT_MYSQL_USER = "hoshuuch"
DEFAULT_MYSQL_PASSWORD = "490229"
DOUYIN_PUBLISH_URL = "https://creator.douyin.com/creator-micro/content/upload"
DEFAULT_PHONE = "15548242598"
SMS_TABLE = "yd_douyin_sms_code"
ACCOUNT_TABLE = "uploader_account_douyin"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Douyin QR login with optional SMS verification and DB code polling.")
    parser.add_argument("--account-key", default="douyin_main")
    parser.add_argument("--phone", default=DEFAULT_PHONE)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=420)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--work-dir", default="/private/tmp/ydbi/douyin-qr-login")
    parser.add_argument("--mysql-host", default=os.getenv("YDBI_MYSQL_HOST", DEFAULT_MYSQL_HOST))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("YDBI_MYSQL_PORT", str(DEFAULT_MYSQL_PORT))))
    parser.add_argument("--mysql-database", default=os.getenv("YDBI_MYSQL_DATABASE", DEFAULT_MYSQL_DATABASE))
    parser.add_argument("--mysql-user", default=os.getenv("YDBI_MYSQL_USER", DEFAULT_MYSQL_USER))
    parser.add_argument("--mysql-password", default=os.getenv("YDBI_MYSQL_PASSWORD", DEFAULT_MYSQL_PASSWORD))
    return parser.parse_args()


def normalize_account_key(value: str) -> str:
    text = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", text):
        raise SystemExit(f"Invalid account key: {value!r}")
    return text


def connect_db(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        connection_timeout=10,
    )


def ensure_schema(connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SMS_TABLE} (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            platform VARCHAR(32) NOT NULL,
            account_key VARCHAR(64) NOT NULL,
            purpose VARCHAR(64) NOT NULL,
            phone VARCHAR(32) NOT NULL,
            code VARCHAR(16) NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'requested',
            requested_at DATETIME NULL,
            consumed_at DATETIME NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_douyin_sms_lookup (platform, account_key, purpose, phone, status, updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ACCOUNT_TABLE} (
          account_key VARCHAR(64) NOT NULL PRIMARY KEY,
          user_id VARCHAR(128) NULL,
          nickname VARCHAR(128) NULL,
          storage_state_json MEDIUMTEXT NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    connection.commit()


def visible_qr_data_url(page) -> str:
    value = page.evaluate(
        """
        () => {
          const visible = (el) => {
            const rect = el.getBoundingClientRect();
            const style = window.getComputedStyle(el);
            return rect.width >= 120 && rect.height >= 120 &&
              style.visibility !== 'hidden' && style.display !== 'none' &&
              rect.bottom > 0 && rect.right > 0 &&
              rect.top < window.innerHeight && rect.left < window.innerWidth;
          };
          let best = '';
          let bestScore = -1;
          const scoreRect = (rect) => {
            const ratio = rect.width / rect.height;
            if (ratio < 0.82 || ratio > 1.22) return -1;
            return Math.min(rect.width, rect.height);
          };
          for (const img of Array.from(document.querySelectorAll('img'))) {
            if (!visible(img)) continue;
            const rect = img.getBoundingClientRect();
            let score = scoreRect(rect);
            if (score < 0) continue;
            const src = img.getAttribute('src') || '';
            if (src.startsWith('data:image')) score += 2000;
            if ((img.alt || '').includes('二维码')) score += 500;
            if (score > bestScore) {
              best = src;
              bestScore = score;
            }
          }
          for (const canvas of Array.from(document.querySelectorAll('canvas'))) {
            if (!visible(canvas)) continue;
            const rect = canvas.getBoundingClientRect();
            let score = scoreRect(rect);
            if (score < 0) continue;
            try {
              const src = canvas.toDataURL('image/png');
              score += 2200;
              if (score > bestScore) {
                best = src;
                bestScore = score;
              }
            } catch (_) {}
          }
          return best;
        }
        """
    )
    return str(value or "")


def write_qr_image(page, path: Path) -> None:
    deadline = time.time() + 30
    data_url = ""
    while time.time() < deadline:
        data_url = visible_qr_data_url(page)
        if data_url:
            break
        page.wait_for_timeout(500)
    if not data_url:
        page.screenshot(path=path, full_page=True)
        return
    if data_url.startswith("data:image"):
        payload = data_url.split(",", 1)[1]
        path.write_bytes(base64.b64decode(payload))
        return
    page.locator("img").first.screenshot(path=path)


def open_file(path: Path) -> None:
    subprocess.run(["open", str(path)], check=False)


def dump(page, work_dir: Path, label: str) -> None:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    screenshot = work_dir / f"{stamp}-{label}.png"
    html = work_dir / f"{stamp}-{label}.html"
    try:
        page.screenshot(path=screenshot, full_page=True, timeout=10000)
        html.write_text(page.content(), encoding="utf-8")
        print(f"diagnostic label={label} screenshot={screenshot} html={html}", flush=True)
    except Exception as exc:
        print(f"diagnostic failed label={label} message={exc}", flush=True)


def click_text_if_visible(page, *texts: str) -> bool:
    for text in texts:
        try:
            locator = page.get_by_text(text).first
            if locator.count() > 0 and locator.is_visible():
                locator.click(timeout=3000)
                page.wait_for_timeout(500)
                return True
        except Exception:
            pass
    return False


def fill_phone_if_needed(page, phone: str) -> None:
    for selector in ["input[name='normal-input']", "input[placeholder*='手机号']", "input[type='tel']"]:
        try:
            locator = page.locator(selector).first
            if locator.count() > 0:
                value = locator.input_value(timeout=1000)
                if not value or len(value) < 6:
                    locator.fill(phone)
                    page.wait_for_timeout(500)
                return
        except Exception:
            pass


def click_send_sms(page) -> bool:
    for text in ["获取验证码", "发送验证码", "获取短信验证码"]:
        try:
            locator = page.get_by_text(text).first
            if locator.count() > 0 and locator.is_visible():
                locator.click(timeout=5000)
                page.wait_for_timeout(2000)
                return True
        except Exception:
            pass
    try:
        locator = page.locator("button:has-text('验证码')").first
        if locator.count() > 0:
            locator.click(timeout=5000)
            page.wait_for_timeout(2000)
            return True
    except Exception:
        pass
    return False


def click_identity_sms_option(page) -> bool:
    for text in ["接收短信验证码", "发送短信验证"]:
        try:
            locator = page.get_by_text(text).first
            if locator.count() > 0 and locator.is_visible():
                locator.click(timeout=5000)
                page.wait_for_timeout(2000)
                return True
        except Exception:
            pass
    return False


def create_sms_request(connection, account_key: str, phone: str) -> int:
    cursor = connection.cursor()
    cursor.execute(
        f"INSERT INTO {SMS_TABLE} (platform, account_key, purpose, phone, status, requested_at) VALUES (%s, %s, %s, %s, %s, NOW())",
        ("douyin", account_key, "login_verify", phone, "requested"),
    )
    connection.commit()
    return int(cursor.lastrowid)


def wait_for_sms_code(connection, account_key: str, phone: str, requested_at: datetime, timeout_seconds: int, poll_seconds: int) -> tuple[int, str] | None:
    deadline = time.time() + timeout_seconds
    cursor = connection.cursor(dictionary=True)
    while time.time() < deadline:
        cursor.execute(
            f"""
            SELECT id, code FROM {SMS_TABLE}
            WHERE platform = 'douyin'
              AND account_key = %s
              AND purpose = 'login_verify'
              AND phone = %s
              AND status IN ('requested', 'pending')
              AND code IS NOT NULL
              AND code <> ''
              AND updated_at >= %s
              AND updated_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (account_key, phone, requested_at),
        )
        row = cursor.fetchone()
        if row:
            return int(row["id"]), str(row["code"])
        print("waiting sms code from DB...", flush=True)
        time.sleep(poll_seconds)
    return None


def consume_sms_code(connection, sms_id: int) -> None:
    cursor = connection.cursor()
    cursor.execute(f"UPDATE {SMS_TABLE} SET status='consumed', consumed_at=NOW(), updated_at=NOW() WHERE id=%s", (sms_id,))
    connection.commit()


def submit_sms_code(page, code: str) -> None:
    for selector in ["input[name='button-input']", "input[placeholder*='验证码']", "input[placeholder*='短信']"]:
        locator = page.locator(selector).first
        if locator.count() > 0:
            locator.fill(code)
            break
    click_text_if_visible(page, "登录", "验证", "确认", "下一步")
    page.wait_for_timeout(3000)


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
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if name in {"passport_assist_user", "sso_uid_tt", "uid_tt"}:
            user_id = first_text(user_id, value)
    return (user_id[:128] or None, nickname[:128] or None)


def save_storage_state(connection, account_key: str, state: dict[str, Any]) -> None:
    user_id, nickname = profile_from_storage_state(state)
    storage_state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    cursor = connection.cursor()
    cursor.execute(
        f"""
        INSERT INTO {ACCOUNT_TABLE} (account_key, user_id, nickname, storage_state_json, updated_at)
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
    print(f"saved storage state account_key={account_key} cookies={len(state.get('cookies', []))} origins={len(state.get('origins', []))}", flush=True)


def login_completed(page) -> bool:
    try:
        if page.locator("input[type='file']").count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.get_by_text("上传视频").count() > 0 and page.get_by_text("手机号登录").count() == 0:
            return True
    except Exception:
        pass
    return False


def has_sms_gate(page) -> bool:
    for text in ["获取验证码", "发送验证码", "请输入验证码", "验证码登录"]:
        try:
            if page.get_by_text(text).count() > 0:
                return True
        except Exception:
            pass
    for selector in ["input[name='button-input']", "input[placeholder*='验证码']"]:
        try:
            if page.locator(selector).count() > 0:
                return True
        except Exception:
            pass
    return False


def main() -> int:
    args = parse_args()
    account_key = normalize_account_key(args.account_key)
    work_dir = Path(args.work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    connection = connect_db(args)
    ensure_schema(connection)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            channel="chrome",
            headless=args.headless,
            args=["--disable-blink-features=AutomationControlled", "--lang=zh-CN"],
        )
        context = browser.new_context(permissions=["geolocation"])
        page = context.new_page()
        page.goto(DOUYIN_PUBLISH_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)
        click_text_if_visible(page, "扫码登录")
        qr_path = work_dir / "douyin-login-qr.png"
        write_qr_image(page, qr_path)
        dump(page, work_dir, "qr-ready")
        open_file(qr_path)
        print(f"QR opened: {qr_path}", flush=True)

        started = time.time()
        sms_requested = False
        while time.time() - started < args.timeout_seconds:
            page.wait_for_timeout(args.poll_seconds * 1000)
            dump(page, work_dir, "poll")
            if login_completed(page):
                save_storage_state(connection, account_key, context.storage_state())
                print("login completed", flush=True)
                browser.close()
                return 0
            if click_identity_sms_option(page):
                dump(page, work_dir, "identity-sms-option-clicked")
                if login_completed(page):
                    save_storage_state(connection, account_key, context.storage_state())
                    print("login completed", flush=True)
                    browser.close()
                    return 0
            if has_sms_gate(page) and not sms_requested:
                fill_phone_if_needed(page, args.phone)
                if click_send_sms(page):
                    dump(page, work_dir, "sms-sent-or-blocked")
                    requested_at = datetime.now() - timedelta(seconds=5)
                    sms_id = create_sms_request(connection, account_key, args.phone)
                    sms_requested = True
                    print(f"sms requested id={sms_id} phone={args.phone}", flush=True)
                    result = wait_for_sms_code(connection, account_key, args.phone, requested_at, 180, args.poll_seconds)
                    if result is None:
                        print("sms code timeout", flush=True)
                        continue
                    code_id, code = result
                    submit_sms_code(page, code)
                    consume_sms_code(connection, code_id)
                    dump(page, work_dir, "sms-submitted")
        dump(page, work_dir, "timeout")
        browser.close()
        raise SystemExit("Timed out waiting for Douyin QR login")


if __name__ == "__main__":
    raise SystemExit(main())
