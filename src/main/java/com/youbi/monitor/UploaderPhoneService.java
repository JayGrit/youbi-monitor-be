package com.youbi.monitor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UploaderPhoneService {
    private static final String TABLE = "uploader_phone";
    private static final String ACCOUNT_TABLE = "uploader_phone_account";
    private static final List<PlatformTable> PLATFORMS = List.of(
            new PlatformTable("douyin", "uploader_account_douyin", "douyin_account_id"),
            new PlatformTable("xiaohongshu", "uploader_account_xiaohongshu", "xiaohongshu_account_id"),
            new PlatformTable("bilibili", "uploader_account_bilibili", "bilibili_account_id"),
            new PlatformTable("shipinhao", "uploader_account_shipinhao", "shipinhao_account_id"),
            new PlatformTable("kuaishou", "uploader_account_kuaishou", "kuaishou_account_id"),
            new PlatformTable("jinritoutiao", "uploader_account_jinritoutiao", "jinritoutiao_account_id")
    );

    private final JdbcTemplate jdbcTemplate;

    public UploaderPhoneService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        ensureSchema();
    }

    public UploaderPhoneMatrixResponse matrix() {
        return new UploaderPhoneMatrixResponse(phones(), platformAccounts());
    }

    public UploaderPhoneRecord updatePlatformAccount(long phoneId, String platform, UploaderPhoneAccountUpdateRequest request) throws IOException {
        PlatformTable platformTable = platformTable(platform);
        Long accountId = request == null ? null : request.accountId();
        String note = request == null ? "" : TextSupport.text(request.note());
        if (accountId != null && accountId <= 0) {
            accountId = null;
        }
        if (accountId != null && !accountIdExists(platformTable, accountId)) {
            throw new IOException("Uploader account not found: " + platform + "/" + accountId);
        }
        if (!phoneExists(phoneId)) {
            throw new IOException("Uploader phone not found: " + phoneId);
        }
        jdbcTemplate.update(
                "UPDATE uploader_phone SET note = ?, updated_at = NOW() WHERE id = ?",
                note.isBlank() ? null : note,
                phoneId
        );
        if (accountId == null) {
            jdbcTemplate.update(
                    "DELETE FROM uploader_phone_account WHERE phone_id = ? AND platform = ?",
                    phoneId,
                    platformTable.key()
            );
        } else {
            jdbcTemplate.update(
                    """
                    INSERT INTO uploader_phone_account (phone_id, platform, account_id, updated_at)
                    VALUES (?, ?, ?, NOW())
                    ON DUPLICATE KEY UPDATE account_id = VALUES(account_id), updated_at = NOW()
                    """,
                    phoneId,
                    platformTable.key(),
                    accountId
            );
        }
        return phone(phoneId);
    }

    private List<UploaderPhoneRecord> phones() {
        Map<Long, Map<String, Long>> accountsByPhone = phoneAccountBindings();
        return jdbcTemplate.query(
                """
                SELECT id, phone
                     , remark
                     , note
                FROM uploader_phone
                ORDER BY id
                """,
                (rs, rowNum) -> new UploaderPhoneRecord(
                        rs.getLong("id"),
                        rs.getString("phone"),
                        rs.getString("remark"),
                        rs.getString("note"),
                        accountsByPhone.getOrDefault(rs.getLong("id"), Map.of())
                )
        );
    }

    private UploaderPhoneRecord phone(long phoneId) throws IOException {
        Map<Long, Map<String, Long>> accountsByPhone = phoneAccountBindings();
        List<UploaderPhoneRecord> rows = jdbcTemplate.query(
                """
                SELECT id, phone
                     , remark
                     , note
                FROM uploader_phone
                WHERE id = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new UploaderPhoneRecord(
                        rs.getLong("id"),
                        rs.getString("phone"),
                        rs.getString("remark"),
                        rs.getString("note"),
                        accountsByPhone.getOrDefault(rs.getLong("id"), Map.of())
                ),
                phoneId
        );
        return rows.stream().findFirst().orElseThrow(() -> new IOException("Uploader phone not found: " + phoneId));
    }

    private Map<Long, Map<String, Long>> phoneAccountBindings() {
        Map<Long, Map<String, Long>> result = new HashMap<>();
        jdbcTemplate.query(
                """
                SELECT phone_id, platform, account_id
                FROM uploader_phone_account
                ORDER BY phone_id, platform
                """,
                rs -> {
                    result.computeIfAbsent(rs.getLong("phone_id"), ignored -> new HashMap<>())
                            .put(rs.getString("platform"), rs.getLong("account_id"));
                }
        );
        return result;
    }

    private List<UploaderPhonePlatformAccounts> platformAccounts() {
        return PLATFORMS.stream()
                .map(platform -> new UploaderPhonePlatformAccounts(platform.key(), accountOptions(platform)))
                .toList();
    }

    private List<UploaderPhoneAccountOption> accountOptions(PlatformTable platform) {
        if (!tableExists(platform.table())) {
            return List.of();
        }
        AccountTableSchemaSupport.ensureSurrogatePrimaryKey(jdbcTemplate, platform.table());
        ensureAccountColumn(platform.table(), "display_name", "VARCHAR(128) NULL");
        ensureAccountColumn(platform.table(), "avatar_url", "VARCHAR(1024) NULL");
        String nameExpression = "COALESCE(NULLIF(nickname, ''), account_key)";
        if ("bilibili".equals(platform.key())) {
            nameExpression = "COALESCE(NULLIF(uname, ''), account_key)";
        }
        return jdbcTemplate.query(
                ("""
                SELECT id, account_key, %s AS display_name, display_name AS remark, avatar_url
                FROM %s
                ORDER BY account_key
                """).formatted(nameExpression, platform.table()),
                (rs, rowNum) -> new UploaderPhoneAccountOption(
                        rs.getLong("id"),
                        rs.getString("account_key"),
                        rs.getString("display_name"),
                        rs.getString("remark"),
                        rs.getString("avatar_url")
                )
        );
    }

    private boolean accountIdExists(PlatformTable platform, long accountId) {
        if (!tableExists(platform.table())) {
            return false;
        }
        AccountTableSchemaSupport.ensureSurrogatePrimaryKey(jdbcTemplate, platform.table());
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + platform.table() + " WHERE id = ?",
                Integer.class,
                accountId
        );
        return count != null && count > 0;
    }

    private boolean phoneExists(long phoneId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM uploader_phone WHERE id = ?",
                Integer.class,
                phoneId
        );
        return count != null && count > 0;
    }

    private void ensureSchema() {
        for (PlatformTable platform : PLATFORMS) {
            if (tableExists(platform.table())) {
                AccountTableSchemaSupport.ensureSurrogatePrimaryKey(jdbcTemplate, platform.table());
            }
        }
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_phone (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    phone VARCHAR(32) NOT NULL,
                    remark VARCHAR(64) NULL,
                    note VARCHAR(255) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_phone_phone (phone)
                )
                """
        );
        ensureUploaderPhoneColumn("remark", "VARCHAR(64) NULL");
        ensureUploaderPhoneColumn("note", "VARCHAR(255) NULL");
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_phone_account (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    phone_id BIGINT NOT NULL,
                    platform VARCHAR(32) NOT NULL,
                    account_id BIGINT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_phone_account_phone_platform (phone_id, platform),
                    KEY idx_uploader_phone_account_platform_account (platform, account_id),
                    KEY idx_uploader_phone_account_phone (phone_id)
                )
                """
        );
        migrateLegacyAccountColumns();
        dropLegacyPhoneColumns();
        seedPhone("15548242598", "主号");
        seedPhone("15049190018", "流量");
        seedPhone("19139952929", "小宝");
    }

    private void migrateLegacyAccountColumns() {
        for (PlatformTable platform : PLATFORMS) {
            if (!columnExists(TABLE, platform.phoneColumn())) {
                continue;
            }
            jdbcTemplate.update(
                    ("""
                    INSERT INTO uploader_phone_account (phone_id, platform, account_id, updated_at)
                    SELECT id, ?, %s, NOW()
                    FROM uploader_phone
                    WHERE %s IS NOT NULL
                    ON DUPLICATE KEY UPDATE account_id = VALUES(account_id), updated_at = NOW()
                    """).formatted(platform.phoneColumn(), platform.phoneColumn()),
                    platform.key()
            );
        }
    }

    private void dropLegacyPhoneColumns() {
        for (PlatformTable platform : PLATFORMS) {
            dropColumnIfExists(TABLE, platform.phoneColumn());
        }
    }

    private void seedPhone(String phone, String remark) {
        jdbcTemplate.update(
                """
                INSERT INTO uploader_phone (phone, remark, note, updated_at)
                VALUES (?, ?, NULL, NOW())
                ON DUPLICATE KEY UPDATE remark = VALUES(remark)
                """,
                phone,
                remark
        );
    }

    private void ensureUploaderPhoneColumn(String column, String definition) {
        if (!columnExists(TABLE, column)) {
            jdbcTemplate.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + column + " " + definition);
        }
    }

    private boolean columnExists(String table, String column) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        return count != null && count > 0;
    }

    private void dropColumnIfExists(String table, String column) {
        if (columnExists(table, column)) {
            jdbcTemplate.execute("ALTER TABLE " + table + " DROP COLUMN " + column);
        }
    }

    private void ensureAccountColumn(String table, String column, String definition) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        if (count == null || count == 0) {
            jdbcTemplate.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
    }

    private boolean tableExists(String table) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                """,
                Integer.class,
                table
        );
        return count != null && count > 0;
    }

    private PlatformTable platformTable(String platform) throws IOException {
        String normalized = TextSupport.text(platform);
        return PLATFORMS.stream()
                .filter(item -> item.key().equals(normalized))
                .findFirst()
                .orElseThrow(() -> new IOException("Unsupported platform: " + platform));
    }

    private record PlatformTable(String key, String table, String phoneColumn) {
    }
}

record UploaderPhoneMatrixResponse(
        List<UploaderPhoneRecord> phones,
        List<UploaderPhonePlatformAccounts> platforms
) {
}

record UploaderPhoneRecord(
        Long id,
        String phone,
        String remark,
        String note,
        Map<String, Long> accounts
) {
}

record UploaderPhonePlatformAccounts(
        String platform,
        List<UploaderPhoneAccountOption> accounts
) {
}

record UploaderPhoneAccountOption(
        Long id,
        String accountKey,
        String displayName,
        String remark,
        String avatarUrl
) {
}

record UploaderPhoneAccountUpdateRequest(
        Long accountId,
        String note
) {
}
