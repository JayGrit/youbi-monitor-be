package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.repository.IUploaderAccountRepositoryService;
import com.youbi.monitor.repository.UploaderAccountRepository;
import jakarta.annotation.PostConstruct;
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

    @PostConstruct
    public void ensureSchema() {
        ensureDeprecatedColumn();
    }

    @Override
    public Optional<UploaderAccountState> state(String platform, String topic) {
        if (!tableExists(TABLE)) {
            return Optional.empty();
        }
        String downloaderMaxStagedCountSql = columnExists(TABLE, "downloader_max_staged_count")
                ? "downloader_max_staged_count"
                : "5 downloader_max_staged_count";
        String quietStartSql = columnExists(TABLE, "upload_quiet_start_time")
                ? "upload_quiet_start_time"
                : "TIME('01:00:00') upload_quiet_start_time";
        String quietEndSql = columnExists(TABLE, "upload_quiet_end_time")
                ? "upload_quiet_end_time"
                : "TIME('07:00:00') upload_quiet_end_time";
        List<UploaderAccountState> rows = repository.query(
                ("""
                SELECT platform, topic, last_upload_at, next_upload_allowed_at,
                       upload_cooldown_min_seconds, upload_cooldown_max_seconds,
                       %s, %s,
                       %s,
                       (
                         SELECT COUNT(*)
                         FROM downloader_submission submission
                         WHERE submission.topic = uploader_account.topic
                           AND submission.status = 'ready'
                       ) downloader_pending_count,
                       (
                         SELECT COUNT(*)
                         FROM downloader_submission submission
                         JOIN task task ON task.id = submission.task_id
                         WHERE submission.topic = uploader_account.topic
                           AND submission.status = 'success'
                           AND NULLIF(submission.task_id, '') IS NOT NULL
                           AND task.status <> 'failed'
                           AND NOT EXISTS (
                             SELECT 1
                             FROM uploader_task upload_submission
                             WHERE upload_submission.task_id = submission.task_id
                               AND upload_submission.topic = submission.topic
                           )
                       ) staged_running_count,
                       (
                         SELECT COUNT(*)
                         FROM downloader_submission submission
                         JOIN task task ON task.id = submission.task_id
                         WHERE submission.topic = uploader_account.topic
                           AND submission.status = 'success'
                           AND NULLIF(submission.task_id, '') IS NOT NULL
                           AND task.status = 'failed'
                           AND NOT EXISTS (
                             SELECT 1
                             FROM uploader_task upload_submission
                             WHERE upload_submission.task_id = submission.task_id
                               AND upload_submission.topic = submission.topic
                           )
                       ) staged_failed_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = uploader_account.platform
                           AND upload_submission.topic = uploader_account.topic
                           AND upload_submission.status = 'success'
                           AND DATE(upload_submission.completed_at) = CURDATE()
                       ) today_upload_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = uploader_account.platform
                           AND upload_submission.topic = uploader_account.topic
                           AND upload_submission.status = 'ready'
                       ) cooldown_waiting_count,
                       (
                         SELECT upload_submission.task_id
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = uploader_account.platform
                           AND upload_submission.topic = uploader_account.topic
                           AND upload_submission.status = 'running'
                         ORDER BY upload_submission.started_at DESC, upload_submission.id DESC
                         LIMIT 1
                       ) upload_running_task_id,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = uploader_account.platform
                           AND upload_submission.topic = uploader_account.topic
                           AND upload_submission.status = 'running'
                       ) upload_running_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = uploader_account.platform
                           AND upload_submission.topic = uploader_account.topic
                           AND upload_submission.status = 'failed'
                       ) failed_upload_count,
                       is_enabled, is_available, source_table, source_updated_at
                FROM uploader_account
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                LIMIT 1
                """).formatted(quietStartSql, quietEndSql, downloaderMaxStagedCountSql),
                (rs, rowNum) -> new UploaderAccountState(
                        rs.getString("platform"),
                        rs.getString("topic"),
                        toLocalDateTime(rs.getTimestamp("last_upload_at")),
                        toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                        nullableInt(rs, "upload_cooldown_min_seconds"),
                        nullableInt(rs, "upload_cooldown_max_seconds"),
                        toLocalTime(rs.getTime("upload_quiet_start_time")),
                        toLocalTime(rs.getTime("upload_quiet_end_time")),
                        rs.getInt("downloader_max_staged_count"),
                        rs.getInt("downloader_pending_count"),
                        rs.getInt("staged_running_count"),
                        rs.getInt("staged_failed_count"),
                        rs.getInt("today_upload_count"),
                        rs.getInt("cooldown_waiting_count"),
                        rs.getString("upload_running_task_id"),
                        rs.getInt("upload_running_count"),
                        rs.getInt("failed_upload_count"),
                        rs.getBoolean("is_enabled"),
                        nullableBoolean(rs, "is_available"),
                        rs.getString("source_table"),
                        toLocalDateTime(rs.getTimestamp("source_updated_at"))
                ),
                normalizePlatform(platform),
                normalizeTopic(topic)
        );
        return rows.stream().findFirst();
    }

    @Override
    public boolean renameTopic(String platform, String oldTopic, String newTopic) {
        ensureDeprecatedColumn();
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedOldTopic = normalizeTopic(oldTopic);
        String normalizedNewTopic = normalizeTopic(newTopic);
        Integer existing = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM uploader_account
                WHERE platform = ? AND topic = ?
                """,
                Integer.class,
                normalizedPlatform,
                normalizedNewTopic
        );
        if (existing != null && existing > 0) {
            return false;
        }
        int inserted = repository.update(
                """
                INSERT INTO uploader_account (
                    platform,
                    topic,
                    source_table,
                    source_updated_at,
                    last_upload_at,
                    next_upload_allowed_at,
                    upload_cooldown_min_seconds,
                    upload_cooldown_max_seconds,
                    is_enabled,
                    is_available,
                    downloader_max_staged_count,
                    upload_quiet_start_time,
                    upload_quiet_end_time,
                    is_deprecated,
                    created_at,
                    updated_at
                )
                SELECT platform,
                       ?,
                       source_table,
                       source_updated_at,
                       NULL,
                       NULL,
                       upload_cooldown_min_seconds,
                       upload_cooldown_max_seconds,
                       is_enabled,
                       is_available,
                       downloader_max_staged_count,
                       upload_quiet_start_time,
                       upload_quiet_end_time,
                       0,
                       NOW(),
                       NOW()
                FROM uploader_account
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                normalizedNewTopic,
                normalizedPlatform,
                normalizedOldTopic
        );
        if (inserted != 1) {
            return false;
        }
        int deprecated = repository.update(
                """
                UPDATE uploader_account
                SET is_deprecated = 1, updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                normalizedPlatform,
                normalizedOldTopic
        );
        return deprecated == 1;
    }

    @Override
    public boolean updateEnabled(String platform, String topic, boolean enabled) {
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET is_enabled = ?, updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                enabled,
                normalizePlatform(platform),
                normalizeTopic(topic)
        );
        return updated == 1;
    }

    @Override
    public boolean updateAvailable(String platform, String topic, boolean available) {
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET is_available = ?, updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                available,
                normalizePlatform(platform),
                normalizeTopic(topic)
        );
        return updated == 1;
    }

    @Override
    public boolean updateCooldown(String platform, String topic, int minSeconds, int maxSeconds) {
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET upload_cooldown_min_seconds = ?,
                    upload_cooldown_max_seconds = ?,
                    updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                minSeconds,
                maxSeconds,
                normalizePlatform(platform),
                normalizeTopic(topic)
        );
        return updated == 1;
    }

    @Override
    public boolean updateQuietTime(String platform, String topic, LocalTime startTime, LocalTime endTime) {
        ensureQuietTimeColumns();
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET upload_quiet_start_time = ?,
                    upload_quiet_end_time = ?,
                    updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                Time.valueOf(startTime),
                Time.valueOf(endTime),
                normalizePlatform(platform),
                normalizeTopic(topic)
        );
        return updated == 1;
    }

    @Override
    public boolean updateDownloaderMaxStagedCount(String platform, String topic, int maxStagedCount) {
        ensureDownloaderStagingColumns();
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET downloader_max_staged_count = ?,
                    updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                maxStagedCount,
                normalizePlatform(platform),
                normalizeTopic(topic)
        );
        return updated == 1;
    }

    private void ensureDeprecatedColumn() {
        if (!tableExists(TABLE) || columnExists(TABLE, "is_deprecated")) {
            return;
        }
        repository.update("""
                ALTER TABLE uploader_account
                ADD COLUMN is_deprecated TINYINT(1) NOT NULL DEFAULT 0
                """);
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

    private static String normalizeTopic(String value) {
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
