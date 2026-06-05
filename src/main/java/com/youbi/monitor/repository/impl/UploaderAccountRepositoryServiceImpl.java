package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.repository.IUploaderAccountRepositoryService;
import com.youbi.monitor.repository.UploaderAccountRepository;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
        String downloaderPendingCountSql = columnExists(TABLE, "downloader_pending_count")
                ? "downloader_pending_count"
                : "0 downloader_pending_count";
        String stagedRunningCountSql = columnExists(TABLE, "staged_running_count")
                ? "staged_running_count"
                : "0 staged_running_count";
        String quietStartSql = columnExists(TABLE, "upload_quiet_start_time")
                ? "upload_quiet_start_time"
                : "TIME('01:00:00') upload_quiet_start_time";
        String quietEndSql = columnExists(TABLE, "upload_quiet_end_time")
                ? "upload_quiet_end_time"
                : "TIME('07:00:00') upload_quiet_end_time";
        List<UploaderAccountState> rows = repository.query(
                ("""
                SELECT platform, account_key, last_upload_at, next_upload_allowed_at,
                       upload_cooldown_min_seconds, upload_cooldown_max_seconds,
                       %s, %s,
                       %s, %s, %s, today_upload_count, cooldown_waiting_count, %s, %s,
                       %s,
                       is_enabled, is_available, source_table, source_updated_at, metrics_updated_at
                FROM uploader_account
                WHERE platform = ? AND account_key = ?
                LIMIT 1
                """).formatted(quietStartSql, quietEndSql, downloaderMaxStagedCountSql, downloaderPendingCountSql, stagedRunningCountSql, runningTaskIdSql, runningCountSql, failedCountSql),
                (rs, rowNum) -> new UploaderAccountState(
                        rs.getString("platform"),
                        rs.getString("account_key"),
                        toLocalDateTime(rs.getTimestamp("last_upload_at")),
                        toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                        nullableInt(rs, "upload_cooldown_min_seconds"),
                        nullableInt(rs, "upload_cooldown_max_seconds"),
                        toLocalTime(rs.getTime("upload_quiet_start_time")),
                        toLocalTime(rs.getTime("upload_quiet_end_time")),
                        rs.getInt("downloader_max_staged_count"),
                        rs.getInt("downloader_pending_count"),
                        rs.getInt("staged_running_count"),
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
    public boolean updateQuietTime(String platform, String accountKey, LocalTime startTime, LocalTime endTime) {
        ensureQuietTimeColumns();
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET upload_quiet_start_time = ?,
                    upload_quiet_end_time = ?,
                    updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                Time.valueOf(startTime),
                Time.valueOf(endTime),
                normalizePlatform(platform),
                normalizeAccountKey(accountKey)
        );
        return updated == 1;
    }

    @Override
    public boolean updateDownloaderMaxStagedCount(String platform, String accountKey, int maxStagedCount) {
        ensureDownloaderStagingColumns();
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

    private void ensureDownloaderStagingColumns() {
        if (!tableExists(TABLE)) {
            return;
        }
        if (!columnExists(TABLE, "downloader_max_staged_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN downloader_max_staged_count INT NOT NULL DEFAULT 5
                    """
            );
        }
        if (!columnExists(TABLE, "downloader_pending_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN downloader_pending_count INT NOT NULL DEFAULT 0
                    """
            );
        }
        if (!columnExists(TABLE, "staged_running_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN staged_running_count INT NOT NULL DEFAULT 0
                    """
            );
        }
    }

    private void ensureQuietTimeColumns() {
        if (!tableExists(TABLE)) {
            return;
        }
        if (!columnExists(TABLE, "upload_quiet_start_time")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN upload_quiet_start_time TIME NOT NULL DEFAULT '01:00:00'
                    """
            );
        }
        if (!columnExists(TABLE, "upload_quiet_end_time")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN upload_quiet_end_time TIME NOT NULL DEFAULT '07:00:00'
                    """
            );
        }
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

    private static LocalTime toLocalTime(Time time) {
        return time == null ? null : time.toLocalTime();
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
