package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.repository.IUploaderAccountRepositoryService;
import com.youbi.monitor.repository.UploaderAccountRepository;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class UploaderAccountRepositoryServiceImpl implements IUploaderAccountRepositoryService {
    private static final String TABLE = "uploader_account";

    private final UploaderAccountRepository repository;

    public UploaderAccountRepositoryServiceImpl(UploaderAccountRepository repository) {
        this.repository = repository;
    }

    @Override
    public Optional<UploaderAccountState> state(String platform, String accountKey) {
        if (!tableExists(TABLE)) {
            return Optional.empty();
        }
        String failedCountSql = columnExists(TABLE, "failed_upload_count")
                ? "failed_upload_count"
                : "0 failed_upload_count";
        boolean hasRunningTaskId = columnExists(TABLE, "upload_running_task_id");
        String runningTaskIdSql = hasRunningTaskId
                ? "NULLIF(upload_running_task_id, '') AS upload_running_task_id"
                : "NULL AS upload_running_task_id";
        String runningCountSql = hasRunningTaskId
                ? "CASE WHEN NULLIF(upload_running_task_id, '') IS NULL THEN 0 ELSE 1 END AS upload_running_count"
                : "upload_running_count";
        String downloaderMaxStagedCountSql = columnExists(TABLE, "downloader_max_staged_count")
                ? "downloader_max_staged_count"
                : "5 downloader_max_staged_count";
        List<UploaderAccountState> rows = repository.query(
                ("""
                SELECT platform, account_key, last_upload_at, next_upload_allowed_at,
                       upload_cooldown_min_seconds, upload_cooldown_max_seconds,
                       %s, today_upload_count, cooldown_waiting_count, %s, %s,
                       %s,
                       is_enabled, is_available, source_table, source_updated_at, metrics_updated_at
                FROM uploader_account
                WHERE platform = ? AND account_key = ?
                LIMIT 1
                """).formatted(downloaderMaxStagedCountSql, runningTaskIdSql, runningCountSql, failedCountSql),
                (rs, rowNum) -> new UploaderAccountState(
                        rs.getString("platform"),
                        rs.getString("account_key"),
                        toLocalDateTime(rs.getTimestamp("last_upload_at")),
                        toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                        nullableInt(rs, "upload_cooldown_min_seconds"),
                        nullableInt(rs, "upload_cooldown_max_seconds"),
                        rs.getInt("downloader_max_staged_count"),
                        rs.getInt("today_upload_count"),
                        rs.getInt("cooldown_waiting_count"),
                        rs.getString("upload_running_task_id"),
                        rs.getInt("upload_running_count"),
                        rs.getInt("failed_upload_count"),
                        rs.getBoolean("is_enabled"),
                        nullableBoolean(rs, "is_available"),
                        rs.getString("source_table"),
                        toLocalDateTime(rs.getTimestamp("source_updated_at")),
                        toLocalDateTime(rs.getTimestamp("metrics_updated_at"))
                ),
                normalizePlatform(platform),
                normalizeAccountKey(accountKey)
        );
        return rows.stream().findFirst();
    }

    @Override
    public boolean renameAccountKey(String platform, String oldAccountKey, String newAccountKey) {
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET account_key = ?, updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                normalizeAccountKey(newAccountKey),
                normalizePlatform(platform),
                normalizeAccountKey(oldAccountKey)
        );
        return updated == 1;
    }

    @Override
    public boolean updateEnabled(String platform, String accountKey, boolean enabled) {
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET is_enabled = ?, updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                enabled,
                normalizePlatform(platform),
                normalizeAccountKey(accountKey)
        );
        return updated == 1;
    }

    @Override
    public boolean updateCooldown(String platform, String accountKey, int minSeconds, int maxSeconds) {
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET upload_cooldown_min_seconds = ?,
                    upload_cooldown_max_seconds = ?,
                    updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                minSeconds,
                maxSeconds,
                normalizePlatform(platform),
                normalizeAccountKey(accountKey)
        );
        return updated == 1;
    }

    @Override
    public boolean updateDownloaderMaxStagedCount(String platform, String accountKey, int maxStagedCount) {
        ensureDownloaderMaxStagedCountColumn();
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET downloader_max_staged_count = ?,
                    updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                maxStagedCount,
                normalizePlatform(platform),
                normalizeAccountKey(accountKey)
        );
        return updated == 1;
    }

    @Override
    public int resetTodayUploadCounts() {
        if (!tableExists(TABLE)) {
            return 0;
        }
        return repository.update(
                """
                UPDATE uploader_account
                SET today_upload_count = 0,
                    metrics_updated_at = NOW(),
                    updated_at = NOW()
                WHERE today_upload_count <> 0
                """
        );
    }

    private boolean tableExists(String table) {
        Integer count = repository.queryForObject(
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

    private boolean columnExists(String table, String column) {
        Integer count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        return count != null && count > 0;
    }

    private void ensureDownloaderMaxStagedCountColumn() {
        if (!tableExists(TABLE) || columnExists(TABLE, "downloader_max_staged_count")) {
            return;
        }
        repository.update(
                """
                ALTER TABLE uploader_account
                ADD COLUMN downloader_max_staged_count INT NOT NULL DEFAULT 5
                """
        );
    }

    private static String normalizePlatform(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }

    private static String normalizeAccountKey(String value) {
        return value == null ? "" : value.trim();
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static Integer nullableInt(ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Boolean nullableBoolean(ResultSet rs, String column) throws java.sql.SQLException {
        boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : value;
    }
}
