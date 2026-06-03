package com.youbi.monitor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class UploaderPhoneService {
    private static final String TABLE = "uploader_phone";
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

    public UploaderPhoneRecord updatePhone(long phoneId, UploaderPhoneUpdateRequest request) throws IOException {
        String remark = TextSupport.truncate(request == null ? "" : request.remark(), 128);
        String note = TextSupport.truncate(request == null ? "" : request.note(), 2000);
        int updated = jdbcTemplate.update(
                "UPDATE uploader_phone SET remark = ?, note = ?, updated_at = NOW() WHERE id = ?",
                remark.isBlank() ? null : remark,
                note.isBlank() ? null : note,
                phoneId
        );
        if (updated != 1) {
            throw new IOException("Uploader phone not found: " + phoneId);
        }
        return phone(phoneId);
    }

    public UploaderPhoneRecord updatePlatformAccount(long phoneId, String platform, UploaderPhoneAccountUpdateRequest request) throws IOException {
        PlatformTable platformTable = platformTable(platform);
        Long accountId = request == null ? null : request.accountId();
        if (accountId != null && accountId <= 0) {
            accountId = null;
        }
        if (accountId != null && !accountIdExists(platformTable, accountId)) {
            throw new IOException("Uploader account not found: " + platform + "/" + accountId);
        }
        int updated = jdbcTemplate.update(
                "UPDATE uploader_phone SET " + platformTable.phoneColumn() + " = ?, updated_at = NOW() WHERE id = ?",
                accountId,
                phoneId
        );
        if (updated != 1) {
            throw new IOException("Uploader phone not found: " + phoneId);
        }
        return phone(phoneId);
    }

    private List<UploaderPhoneRecord> phones() {
        return jdbcTemplate.query(
                """
                SELECT id, phone, remark, note,
                       douyin_account_id, xiaohongshu_account_id, bilibili_account_id,
                       shipinhao_account_id, kuaishou_account_id, jinritoutiao_account_id
                FROM uploader_phone
                ORDER BY id
                """,
                (rs, rowNum) -> new UploaderPhoneRecord(
                        rs.getLong("id"),
                        rs.getString("phone"),
                        rs.getString("remark"),
                        rs.getString("note"),
                        nullableLong(rs, "douyin_account_id"),
                        nullableLong(rs, "xiaohongshu_account_id"),
                        nullableLong(rs, "bilibili_account_id"),
                        nullableLong(rs, "shipinhao_account_id"),
                        nullableLong(rs, "kuaishou_account_id"),
                        nullableLong(rs, "jinritoutiao_account_id")
                )
        );
    }

    private UploaderPhoneRecord phone(long phoneId) throws IOException {
        List<UploaderPhoneRecord> rows = jdbcTemplate.query(
                """
                SELECT id, phone, remark, note,
                       douyin_account_id, xiaohongshu_account_id, bilibili_account_id,
                       shipinhao_account_id, kuaishou_account_id, jinritoutiao_account_id
                FROM uploader_phone
                WHERE id = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new UploaderPhoneRecord(
                        rs.getLong("id"),
                        rs.getString("phone"),
                        rs.getString("remark"),
                        rs.getString("note"),
                        nullableLong(rs, "douyin_account_id"),
                        nullableLong(rs, "xiaohongshu_account_id"),
                        nullableLong(rs, "bilibili_account_id"),
                        nullableLong(rs, "shipinhao_account_id"),
                        nullableLong(rs, "kuaishou_account_id"),
                        nullableLong(rs, "jinritoutiao_account_id")
                ),
                phoneId
        );
        return rows.stream().findFirst().orElseThrow(() -> new IOException("Uploader phone not found: " + phoneId));
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
        String nameExpression = "COALESCE(NULLIF(display_name, ''), NULLIF(nickname, ''), account_key)";
        if ("bilibili".equals(platform.key())) {
            nameExpression = "COALESCE(NULLIF(display_name, ''), NULLIF(uname, ''), account_key)";
        }
        return jdbcTemplate.query(
                ("""
                SELECT id, account_key, %s AS display_name, avatar_url
                FROM %s
                ORDER BY account_key
                """).formatted(nameExpression, platform.table()),
                (rs, rowNum) -> new UploaderPhoneAccountOption(
                        rs.getLong("id"),
                        rs.getString("account_key"),
                        rs.getString("display_name"),
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
                    remark VARCHAR(128) NULL,
                    note TEXT NULL,
                    douyin_account_id BIGINT NULL,
                    xiaohongshu_account_id BIGINT NULL,
                    bilibili_account_id BIGINT NULL,
                    shipinhao_account_id BIGINT NULL,
                    kuaishou_account_id BIGINT NULL,
                    jinritoutiao_account_id BIGINT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_phone_phone (phone),
                    KEY idx_uploader_phone_douyin_account (douyin_account_id),
                    KEY idx_uploader_phone_xiaohongshu_account (xiaohongshu_account_id),
                    KEY idx_uploader_phone_bilibili_account (bilibili_account_id),
                    KEY idx_uploader_phone_shipinhao_account (shipinhao_account_id),
                    KEY idx_uploader_phone_kuaishou_account (kuaishou_account_id),
                    KEY idx_uploader_phone_jinritoutiao_account (jinritoutiao_account_id)
                )
                """
        );
        ensureColumn("remark", "VARCHAR(128) NULL");
        ensureColumn("note", "TEXT NULL");
        for (PlatformTable platform : PLATFORMS) {
            ensureColumn(platform.phoneColumn(), "BIGINT NULL");
            ensureIndex("idx_uploader_phone_" + platform.key() + "_account", platform.phoneColumn());
        }
        seedPhone("15548242598", "主号");
        seedPhone("15049190018", "流量");
        seedPhone("19139952929", "小宝");
    }

    private void seedPhone(String phone, String remark) {
        jdbcTemplate.update(
                """
                INSERT INTO uploader_phone (phone, remark, updated_at)
                VALUES (?, ?, NOW())
                ON DUPLICATE KEY UPDATE remark = COALESCE(remark, VALUES(remark))
                """,
                phone,
                remark
        );
    }

    private void ensureColumn(String column, String definition) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                TABLE,
                column
        );
        if (count == null || count == 0) {
            jdbcTemplate.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + column + " " + definition);
        }
    }

    private void ensureIndex(String indexName, String column) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND INDEX_NAME = ?
                """,
                Integer.class,
                TABLE,
                indexName
        );
        if (count == null || count == 0) {
            jdbcTemplate.execute("ALTER TABLE " + TABLE + " ADD KEY " + indexName + " (" + column + ")");
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

    private static Long nullableLong(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
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
        Long douyinAccountId,
        Long xiaohongshuAccountId,
        Long bilibiliAccountId,
        Long shipinhaoAccountId,
        Long kuaishouAccountId,
        Long jinritoutiaoAccountId
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
        String avatarUrl
) {
}

record UploaderPhoneUpdateRequest(
        String remark,
        String note
) {
}

record UploaderPhoneAccountUpdateRequest(
        Long accountId
) {
}
