from __future__ import annotations

from typing import Any


DEFAULT_PHONE = "00000000000"


def mark_uploader_account_available(
    cursor: Any,
    platform: str,
    account_key: str,
    source_table: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO uploader_account (
            platform, account_key, source_table, source_updated_at,
            is_enabled, is_available, is_deprecated, updated_at
        )
        VALUES (%s, %s, %s, NOW(), 1, 1, 0, NOW())
        ON DUPLICATE KEY UPDATE
            source_table = VALUES(source_table),
            source_updated_at = VALUES(source_updated_at),
            is_available = 1,
            is_deprecated = 0,
            updated_at = NOW()
        """,
        (platform, account_key, source_table),
    )


def register_uploader_account(
    cursor: Any,
    platform: str,
    account_key: str,
    source_table: str,
    platform_account_id: int,
) -> str:
    mark_uploader_account_available(cursor, platform, account_key, source_table)
    cursor.execute(
        """
        UPDATE uploader_account
        SET is_enabled = 1,
            updated_at = NOW()
        WHERE platform = %s
          AND account_key = %s
        """,
        (platform, account_key),
    )
    cursor.execute(
        """
        SELECT id
        FROM uploader_phone
        WHERE phone = %s
        LIMIT 1
        FOR UPDATE
        """,
        (DEFAULT_PHONE,),
    )
    phone = cursor.fetchone()
    if phone is None:
        raise RuntimeError(
            f"Default uploader phone {DEFAULT_PHONE} does not exist."
        )

    cursor.execute(
        """
        INSERT INTO uploader_phone_account (
            phone_id, platform, account_id, note, disabled, updated_at
        )
        VALUES (%s, %s, %s, 'new script default binding', 0, NOW())
        ON DUPLICATE KEY UPDATE
            account_id = VALUES(account_id),
            note = VALUES(note),
            disabled = 0,
            updated_at = NOW()
        """,
        (phone[0], platform, platform_account_id),
    )
    return DEFAULT_PHONE
