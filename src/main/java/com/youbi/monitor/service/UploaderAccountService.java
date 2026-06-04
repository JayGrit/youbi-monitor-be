package com.youbi.monitor.service;

import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.repository.DatabaseClient;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class UploaderAccountService {
    private static final String TABLE = "uploader_account";
    private static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_task_bilibili",
            "douyin", "uploader_task_douyin",
            "xiaohongshu", "uploader_task_xiaohongshu",
            "shipinhao", "uploader_task_shipinhao",
            "kuaishou", "uploader_task_kuaishou",
            "jinritoutiao", "uploader_task_jinritoutiao"
    );

    private final DatabaseClient jdbcTemplate;

    public UploaderAccountService(DatabaseClient jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        ensureSchema();
    }

    public UploaderAccountState syncFromPlatformRow(
            String platform,
            String accountKey,
            String sourceTable,
            Boolean enabled,
            Integer minSeconds,
            Integer maxSeconds,
            LocalDateTime lastUploadAt,
            LocalDateTime nextUploadAllowedAt,
            LocalDateTime sourceUpdatedAt
    ) {
        String normalizedPlatform = normalize(platform);
        String normalizedKey = normalize(accountKey);
        boolean mergedEnabled = enabled == null
                ? state(normalizedPlatform, normalizedKey).map(UploaderAccountState::enabled).orElse(true)
                : enabled;
        jdbcTemplate.update(
                """
                INSERT INTO uploader_account (
                    platform, account_key, source_table, is_enabled, is_available,
                    upload_cooldown_min_seconds, upload_cooldown_max_seconds,
                    last_upload_at, next_upload_allowed_at, source_updated_at, updated_at
                )
                VALUES (?, ?, ?, ?, COALESCE(?, 1), ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    source_table = VALUES(source_table),
                    is_enabled = VALUES(is_enabled),
                    is_available = IF(VALUES(is_enabled) = 0, 0, COALESCE(is_available, VALUES(is_available))),
                    upload_cooldown_min_seconds = VALUES(upload_cooldown_min_seconds),
                    upload_cooldown_max_seconds = VALUES(upload_cooldown_max_seconds),
                    last_upload_at = COALESCE(last_upload_at, VALUES(last_upload_at)),
                    next_upload_allowed_at = COALESCE(next_upload_allowed_at, VALUES(next_upload_allowed_at)),
                    source_updated_at = VALUES(source_updated_at),
                    updated_at = NOW()
                """,
                normalizedPlatform,
                normalizedKey,
                sourceTable,
                mergedEnabled,
                mergedEnabled,
                minSeconds == null ? 3600 : minSeconds,
                maxSeconds == null ? 7200 : maxSeconds,
                lastUploadAt,
                nextUploadAllowedAt,
                sourceUpdatedAt
        );
        refreshMetricsIfMissing(normalizedPlatform, normalizedKey);
        return state(normalizedPlatform, normalizedKey).orElseGet(() -> UploaderAccountState.defaults(normalizedPlatform, normalizedKey));
    }

    public Optional<UploaderAccountState> state(String platform, String accountKey) {
        List<UploaderAccountState> rows = jdbcTemplate.query(
                """
                SELECT platform, account_key, last_upload_at, next_upload_allowed_at,
                       upload_cooldown_min_seconds, upload_cooldown_max_seconds,
                       today_upload_count, cooldown_waiting_count, upload_running_count,
                       is_enabled, is_available, source_table, source_updated_at, metrics_updated_at
                FROM uploader_account
                WHERE platform = ? AND account_key = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new UploaderAccountState(
                        rs.getString("platform"),
                        rs.getString("account_key"),
                        toLocalDateTime(rs.getTimestamp("last_upload_at")),
                        toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                        nullableInt(rs, "upload_cooldown_min_seconds"),
                        nullableInt(rs, "upload_cooldown_max_seconds"),
                        rs.getInt("today_upload_count"),
                        rs.getInt("cooldown_waiting_count"),
                        rs.getInt("upload_running_count"),
                        rs.getBoolean("is_enabled"),
                        nullableBoolean(rs, "is_available"),
                        rs.getString("source_table"),
                        toLocalDateTime(rs.getTimestamp("source_updated_at")),
                        toLocalDateTime(rs.getTimestamp("metrics_updated_at"))
                ),
                normalize(platform),
                normalize(accountKey)
        );
        return rows.stream().findFirst();
    }

    public UploaderAccountState updateEnabled(String platform, String accountKey, boolean enabled) {
        ensureAccount(platform, accountKey);
        jdbcTemplate.update(
                "UPDATE uploader_account SET is_enabled = ?, is_available = ?, updated_at = NOW() WHERE platform = ? AND account_key = ?",
                enabled,
                enabled,
                normalize(platform),
                normalize(accountKey)
        );
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public UploaderAccountState updateAvailable(String platform, String accountKey, boolean available) {
        ensureAccount(platform, accountKey);
        jdbcTemplate.update(
                "UPDATE uploader_account SET is_available = ?, updated_at = NOW() WHERE platform = ? AND account_key = ?",
                available,
                normalize(platform),
                normalize(accountKey)
        );
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public UploaderAccountState updateCooldown(String platform, String accountKey, Integer minSeconds, Integer maxSeconds) {
        ensureAccount(platform, accountKey);
        int min = minSeconds == null ? 3600 : minSeconds;
        int max = maxSeconds == null ? 7200 : maxSeconds;
        jdbcTemplate.update(
                """
                UPDATE uploader_account
                SET upload_cooldown_min_seconds = ?, upload_cooldown_max_seconds = ?, updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                min,
                max,
                normalize(platform),
                normalize(accountKey)
        );
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public void renameAccount(String platform, String oldKey, String newKey) {
        jdbcTemplate.update(
                """
                UPDATE uploader_account
                SET account_key = ?, updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                normalize(newKey),
                normalize(platform),
                normalize(oldKey)
        );
    }

    private void ensureAccount(String platform, String accountKey) {
        jdbcTemplate.update(
                """
                INSERT INTO uploader_account (platform, account_key, is_enabled, is_available, updated_at)
                VALUES (?, ?, 1, 1, NOW())
                ON DUPLICATE KEY UPDATE updated_at = updated_at
                """,
                normalize(platform),
                normalize(accountKey)
        );
    }

    private void refreshMetricsIfMissing(String platform, String accountKey) {
        List<LocalDateTime> rows = jdbcTemplate.query(
                """
                SELECT metrics_updated_at
                FROM uploader_account
                WHERE platform = ? AND account_key = ?
                LIMIT 1
                """,
                (rs, rowNum) -> toLocalDateTime(rs.getTimestamp("metrics_updated_at")),
                platform,
                accountKey
        );
        if (!rows.isEmpty() && rows.get(0) != null) {
            return;
        }
        refreshMetrics(platform, accountKey);
    }

    private void refreshMetrics(String platform, String accountKey) {
        String taskTable = UPLOADER_TASK_TABLES.get(platform);
        if (taskTable == null || !tableExists(taskTable)) {
            return;
        }
        jdbcTemplate.update(
                ("""
                UPDATE uploader_account ua
                SET today_upload_count = (
                        SELECT COUNT(*)
                        FROM %s s
                        WHERE s.account_key = ua.account_key
                          AND s.status = 'success'
                          AND s.completed_at >= DATE_ADD(
                              CASE
                                  WHEN TIME(NOW()) >= '08:00:00' THEN CURDATE()
                                  ELSE DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                              END,
                              INTERVAL 8 HOUR
                          )
                          AND s.completed_at < DATE_ADD(
                              CASE
                                  WHEN TIME(NOW()) >= '08:00:00' THEN DATE_ADD(CURDATE(), INTERVAL 1 DAY)
                                  ELSE CURDATE()
                              END,
                              INTERVAL 2 HOUR
                          )
                    ),
                    upload_running_count = (
                        SELECT COUNT(*)
                        FROM %s s
                        WHERE s.account_key = ua.account_key
                          AND s.status = 'running'
                    ),
                    cooldown_waiting_count = (
                        SELECT COUNT(*)
                        FROM %s s
                        WHERE s.account_key = ua.account_key
                          AND s.status = 'ready'
                          AND ua.next_upload_allowed_at > NOW()
                    ) + (
                        SELECT COUNT(*)
                        FROM %s s
                        WHERE s.account_key = ua.account_key
                          AND s.status = 'running'
                    ),
                    last_upload_at = COALESCE(
                        ua.last_upload_at,
                        (
                            SELECT MAX(s.completed_at)
                            FROM %s s
                            WHERE s.account_key = ua.account_key
                              AND s.status = 'success'
                        )
                    ),
                    metrics_updated_at = NOW(),
                    updated_at = NOW()
                WHERE ua.platform = ? AND ua.account_key = ?
                """).formatted(taskTable, taskTable, taskTable, taskTable, taskTable),
                platform,
                accountKey
        );
    }

    private boolean tableExists(String table) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                """,
                Integer.class,
                table
        );
        return count != null && count > 0;
    }

    private void ensureSchema() {
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_account (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    platform VARCHAR(32) NOT NULL,
                    account_key VARCHAR(128) NOT NULL,
                    source_table VARCHAR(128) NULL,
                    source_updated_at DATETIME NULL,
                    last_upload_at DATETIME NULL,
                    next_upload_allowed_at DATETIME NULL,
                    upload_cooldown_min_seconds INT NOT NULL DEFAULT 3600,
                    upload_cooldown_max_seconds INT NOT NULL DEFAULT 7200,
                    today_upload_count INT NOT NULL DEFAULT 0,
                    cooldown_waiting_count INT NOT NULL DEFAULT 0,
                    upload_running_count INT NOT NULL DEFAULT 0,
                    is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                    is_available TINYINT(1) NULL,
                    metrics_updated_at DATETIME NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_account_platform_key (platform, account_key),
                    KEY idx_uploader_account_key (account_key),
                    KEY idx_uploader_account_platform_enabled (platform, is_enabled, account_key)
                )
                """
        );
        ensureColumn("source_table", "VARCHAR(128) NULL");
        ensureColumn("source_updated_at", "DATETIME NULL");
        ensureColumn("last_upload_at", "DATETIME NULL");
        ensureColumn("next_upload_allowed_at", "DATETIME NULL");
        ensureColumn("upload_cooldown_min_seconds", "INT NOT NULL DEFAULT 3600");
        ensureColumn("upload_cooldown_max_seconds", "INT NOT NULL DEFAULT 7200");
        ensureColumn("today_upload_count", "INT NOT NULL DEFAULT 0");
        ensureColumn("cooldown_waiting_count", "INT NOT NULL DEFAULT 0");
        ensureColumn("upload_running_count", "INT NOT NULL DEFAULT 0");
        ensureColumn("is_enabled", "TINYINT(1) NOT NULL DEFAULT 1");
        ensureColumn("is_available", "TINYINT(1) NULL");
        ensureColumn("metrics_updated_at", "DATETIME NULL");
    }

    private void ensureColumn(String column, String definition) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """,
                Integer.class,
                TABLE,
                column
        );
        if (count == null || count == 0) {
            jdbcTemplate.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + column + " " + definition);
        }
    }

    private static String normalize(String value) {
        return TextSupport.text(value);
    }

    private static Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Boolean nullableBoolean(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : value;
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
