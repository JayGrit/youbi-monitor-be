package com.youbi.monitor.service;

import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AccountOverviewService {
    private static final List<String> PLATFORMS = List.of(
            "douyin",
            "xiaohongshu",
            "bilibili",
            "shipinhao",
            "kuaishou",
            "jinritoutiao",
            "x",
            "youtube",
            "doubao",
            "notebooklm",
            "chatgpt"
    );

    private final MonitorRepository repository;
    private final BackupperStatusService backupperStatusService;

    public AccountOverviewService(
            MonitorRepository repository,
            BackupperStatusService backupperStatusService
    ) {
        this.repository = repository;
        this.backupperStatusService = backupperStatusService;
    }

    public List<String> types() {
        return repository.queryForList("""
                SELECT DISTINCT topic
                FROM operator_loginstate
                WHERE topic IS NOT NULL
                  AND TRIM(topic) <> ''
                ORDER BY topic
                """, String.class);
    }

    public Map<String, List<Map<String, Object>>> overview() {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        PLATFORMS.forEach(platform -> result.put(platform, new ArrayList<>()));
        repository.query(accountBaseSelectSql("") + """
                ORDER BY FIELD(ol.platform, 'douyin', 'xiaohongshu', 'bilibili', 'shipinhao', 'kuaishou', 'jinritoutiao', 'x', 'youtube', 'doubao', 'notebooklm', 'chatgpt'), ol.topic
                """,
                rs -> result.computeIfAbsent(rs.getString("platform"), ignored -> new ArrayList<>()).add(mapAccount(rs))
        );
        return result;
    }

    public Map<String, List<Map<String, Object>>> overviewStats() {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        PLATFORMS.forEach(platform -> result.put(platform, new ArrayList<>()));
        repository.query(accountStatsSelectSql("") + """
                ORDER BY FIELD(ol.platform, 'douyin', 'xiaohongshu', 'bilibili', 'shipinhao', 'kuaishou', 'jinritoutiao', 'x', 'youtube', 'doubao', 'notebooklm', 'chatgpt'), ol.topic
                """,
                rs -> result.computeIfAbsent(rs.getString("platform"), ignored -> new ArrayList<>()).add(mapAccountStats(rs))
        );
        return result;
    }

    public BackupperStatus latestBackupperStatus() {
        return backupperStatusService.latestBackupperStatus();
    }

    public Map<String, Object> account(String platform, String topic) {
        String normalizedPlatform = requirePlatform(platform);
        String normalizedTopic = requireTopic(topic);
        return findAccount(normalizedPlatform, normalizedTopic);
    }

    @Transactional
    public Map<String, Object> renameTopic(String platform, String topic, String newTopic) {
        String normalizedPlatform = requireManagedPlatform(platform);
        String oldKey = requireTopic(topic);
        String nextKey = requireTopic(newTopic);
        if (oldKey.equals(nextKey)) {
            return findAccount(normalizedPlatform, oldKey);
        }
        if (repository.update("UPDATE operator_loginstate SET topic = ?, updated_at = NOW() WHERE platform = ? AND topic = ?", nextKey, normalizedPlatform, oldKey) != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        repository.update("UPDATE uploader_account SET topic = ?, updated_at = NOW() WHERE platform = ? AND topic = ? AND is_deprecated = 0", nextKey, normalizedPlatform, oldKey);
        return findAccount(normalizedPlatform, nextKey);
    }

    public Map<String, Object> updateEnabled(String platform, String topic, boolean enabled) {
        return updateGenericState(platform, topic, "is_enabled", enabled);
    }

    public Map<String, Object> updateCooldown(String platform, String topic, Integer minSeconds, Integer maxSeconds) {
        String normalizedPlatform = requireManagedPlatform(platform);
        String normalizedTopic = requireTopic(topic);
        int min = minSeconds == null ? 3600 : minSeconds;
        int max = maxSeconds == null ? 7200 : maxSeconds;
        if (min < 0 || max < min) {
            throw new IllegalArgumentException("Invalid cooldown");
        }
        int updated = repository.update("""
                UPDATE uploader_account
                SET upload_cooldown_min_seconds = ?, upload_cooldown_max_seconds = ?, updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """, min, max, normalizedPlatform, normalizedTopic);
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        return findAccount(normalizedPlatform, normalizedTopic);
    }

    public Map<String, Object> updateNextUploadAllowedAt(String platform, String topic, LocalDateTime nextUploadAllowedAt) {
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedTopic = topic == null ? "" : topic.trim();
        if (!PLATFORMS.contains(normalizedPlatform) || normalizedTopic.isBlank()) {
            throw new IllegalArgumentException("Invalid account");
        }
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET next_upload_allowed_at = ?, updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                nextUploadAllowedAt,
                normalizedPlatform,
                normalizedTopic
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        List<Map<String, Object>> rows = repository.query(
                accountBaseSelectSql("AND ol.platform = ? AND ol.topic = ?"),
                (rs, rowNum) -> mapAccount(rs),
                normalizedPlatform,
                normalizedTopic
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    public Map<String, Object> updateQuietTime(String platform, String topic, LocalTime startTime, LocalTime endTime) {
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedTopic = topic == null ? "" : topic.trim();
        if (!PLATFORMS.contains(normalizedPlatform) || normalizedTopic.isBlank()) {
            throw new IllegalArgumentException("Invalid account");
        }
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("Quiet time is required");
        }
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
                normalizedPlatform,
                normalizedTopic
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        List<Map<String, Object>> rows = repository.query(
                accountBaseSelectSql("AND ol.platform = ? AND ol.topic = ?"),
                (rs, rowNum) -> mapAccount(rs),
                normalizedPlatform,
                normalizedTopic
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    public Map<String, Object> updateDownloaderMaxStagedCount(String platform, String topic, Integer maxStagedCount) {
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedTopic = topic == null ? "" : topic.trim();
        int normalizedMax = maxStagedCount == null ? 5 : maxStagedCount;
        if (!PLATFORMS.contains(normalizedPlatform) || normalizedTopic.isBlank()) {
            throw new IllegalArgumentException("Invalid account");
        }
        if (normalizedMax < 0 || normalizedMax > 100) {
            throw new IllegalArgumentException("Invalid downloader max staged count: " + normalizedMax);
        }
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET downloader_max_staged_count = ?, updated_at = NOW()
                WHERE platform = ? AND topic = ? AND is_deprecated = 0
                """,
                normalizedMax,
                normalizedPlatform,
                normalizedTopic
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        List<Map<String, Object>> rows = repository.query(
                accountBaseSelectSql("AND ol.platform = ? AND ol.topic = ?"),
                (rs, rowNum) -> mapAccount(rs),
                normalizedPlatform,
                normalizedTopic
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    private String accountBaseSelectSql(String extraWhere) {
        return """
                SELECT ol.platform,
                       ol.topic,
                       ua.last_upload_at,
                       ua.next_upload_allowed_at,
                       COALESCE(ua.upload_cooldown_min_seconds, 3600) AS upload_cooldown_min_seconds,
                       COALESCE(ua.upload_cooldown_max_seconds, 7200) AS upload_cooldown_max_seconds,
                       COALESCE(ua.upload_quiet_start_time, TIME('01:00:00')) AS upload_quiet_start_time,
                       COALESCE(ua.upload_quiet_end_time, TIME('07:00:00')) AS upload_quiet_end_time,
                       COALESCE(ua.downloader_max_staged_count, 5) AS downloader_max_staged_count,
                       ua.subscribers,
                       ua.new_subscribers,
                       COALESCE(ua.is_enabled, 1) AS is_enabled,
                       ua.is_available AS is_available,
                       NULL AS mid,
                       NULL AS uname,
                       NULL AS user_id,
                       NULL AS nickname,
                       (ol.storage_state_json IS NOT NULL AND TRIM(ol.storage_state_json) <> '') AS cookie_exists,
                       COALESCE(OCTET_LENGTH(ol.storage_state_json), 0) AS cookie_size_bytes,
                       ol.updated_at AS cookie_updated_at,
                       phone_profile.display_name,
                       phone_profile.avatar_url
                FROM operator_loginstate ol
                LEFT JOIN uploader_account ua
                       ON ua.platform = ol.platform
                      AND ua.topic = ol.topic
                      AND ua.is_deprecated = 0
                LEFT JOIN (
                    SELECT platform, account_id,
                           MAX(display_name) AS display_name,
                           MAX(avatar_url) AS avatar_url
                    FROM uploader_phone_account
                    GROUP BY platform, account_id
                ) phone_profile
                       ON phone_profile.platform = ol.platform
                      AND phone_profile.account_id = ol.id
                WHERE ol.platform IN ('douyin', 'xiaohongshu', 'bilibili', 'shipinhao', 'kuaishou', 'jinritoutiao', 'x', 'youtube', 'doubao', 'notebooklm', 'chatgpt')
                %s
                """.formatted(extraWhere == null ? "" : extraWhere);
    }

    private String accountStatsSelectSql(String extraWhere) {
        return """
                SELECT ol.platform,
                       ol.topic,
                       (
                         SELECT COUNT(*)
                         FROM downloader_submission submission
                         WHERE submission.topic = ol.topic
                           AND submission.status = 'ready'
                       ) AS downloader_pending_count,
                       (
                         SELECT COUNT(*)
                         FROM downloader_submission submission
                         JOIN task task ON task.id = submission.task_id
                         WHERE submission.topic = ol.topic
                           AND submission.status = 'success'
                           AND NULLIF(submission.task_id, '') IS NOT NULL
                           AND task.status <> 'failed'
                           AND NOT EXISTS (
                             SELECT 1
                             FROM uploader_task upload_submission
                             WHERE upload_submission.task_id = submission.task_id
                               AND upload_submission.topic = submission.topic
                           )
                       ) AS staged_running_count,
                       (
                         SELECT COUNT(*)
                         FROM downloader_submission submission
                         JOIN task task ON task.id = submission.task_id
                         WHERE submission.topic = ol.topic
                           AND submission.status = 'success'
                           AND NULLIF(submission.task_id, '') IS NOT NULL
                           AND task.status = 'failed'
                           AND NOT EXISTS (
                             SELECT 1
                             FROM uploader_task upload_submission
                             WHERE upload_submission.task_id = submission.task_id
                               AND upload_submission.topic = submission.topic
                           )
                       ) AS staged_failed_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ol.platform
                           AND upload_submission.topic = ol.topic
                           AND upload_submission.status = 'success'
                           AND DATE(upload_submission.completed_at) = CURDATE()
                       ) AS today_upload_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ol.platform
                           AND upload_submission.topic = ol.topic
                           AND upload_submission.status = 'ready'
                       ) AS cooldown_waiting_count,
                       (
                         SELECT upload_submission.task_id
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ol.platform
                           AND upload_submission.topic = ol.topic
                           AND upload_submission.status = 'running'
                         ORDER BY upload_submission.started_at DESC, upload_submission.id DESC
                         LIMIT 1
                       ) AS upload_running_task_id,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ol.platform
                           AND upload_submission.topic = ol.topic
                           AND upload_submission.status = 'running'
                       ) AS upload_running_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ol.platform
                           AND upload_submission.topic = ol.topic
                           AND upload_submission.status = 'failed'
                       ) AS failed_upload_count
                FROM operator_loginstate ol
                WHERE ol.platform IN ('douyin', 'xiaohongshu', 'bilibili', 'shipinhao', 'kuaishou', 'jinritoutiao', 'x', 'youtube', 'doubao', 'notebooklm', 'chatgpt')
                %s
                """.formatted(extraWhere == null ? "" : extraWhere);
    }

    private Map<String, Object> mapAccount(ResultSet rs) throws java.sql.SQLException {
        String platform = rs.getString("platform");
        boolean cookieExists = rs.getBoolean("cookie_exists");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("storage", "database");
        row.put("topic", rs.getString("topic"));
        row.put("cookieExists", cookieExists);
        row.put("cookieSizeBytes", rs.getLong("cookie_size_bytes"));
        row.put("cookieUpdatedAt", toLocalDateTime(rs.getTimestamp("cookie_updated_at")));
        if ("bilibili".equals(platform)) {
            row.put("mid", nullableLong(rs, "mid"));
            row.put("uname", rs.getString("uname"));
            row.put("face", null);
            row.put("level", null);
        } else {
            row.put("userId", rs.getString("user_id"));
            row.put("nickname", rs.getString("nickname"));
        }
        row.put("lastUploadAt", toLocalDateTime(rs.getTimestamp("last_upload_at")));
        row.put("nextUploadAllowedAt", toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")));
        row.put("uploadCooldownMinSeconds", nullableInt(rs, "upload_cooldown_min_seconds"));
        row.put("uploadCooldownMaxSeconds", nullableInt(rs, "upload_cooldown_max_seconds"));
        row.put("uploadQuietStartTime", toLocalTime(rs.getTime("upload_quiet_start_time")));
        row.put("uploadQuietEndTime", toLocalTime(rs.getTime("upload_quiet_end_time")));
        row.put("downloaderMaxStagedCount", rs.getInt("downloader_max_staged_count"));
        row.put("subscribers", nullableLong(rs, "subscribers"));
        row.put("newSubscribers", nullableLong(rs, "new_subscribers"));
        row.put("statsLoading", true);
        row.put("enabled", rs.getBoolean("is_enabled"));
        row.put("available", nullableBoolean(rs, "is_available"));
        row.put("valid", null);
        row.put("message", cookieExists ? "已保存" : "未登录");
        row.put("raw", Map.of());
        row.put("displayName", rs.getString("display_name"));
        row.put("avatarUrl", rs.getString("avatar_url"));
        return row;
    }

    private Map<String, Object> mapAccountStats(ResultSet rs) throws java.sql.SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("platform", rs.getString("platform"));
        row.put("topic", rs.getString("topic"));
        row.put("downloaderPendingCount", rs.getInt("downloader_pending_count"));
        row.put("stagedRunningCount", rs.getInt("staged_running_count"));
        row.put("stagedFailedCount", rs.getInt("staged_failed_count"));
        row.put("todayUploadCount", rs.getInt("today_upload_count"));
        row.put("cooldownWaitingCount", rs.getInt("cooldown_waiting_count"));
        row.put("uploadRunningTaskId", rs.getString("upload_running_task_id"));
        row.put("uploadRunningCount", rs.getInt("upload_running_count"));
        row.put("failedUploadCount", rs.getInt("failed_upload_count"));
        row.put("statsLoading", false);
        return row;
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

    private static Long nullableLong(ResultSet rs, String column) throws java.sql.SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Boolean nullableBoolean(ResultSet rs, String column) throws java.sql.SQLException {
        boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : value;
    }

    private static String normalizePlatform(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }

    private String requirePlatform(String platform) {
        String normalized = normalizePlatform(platform);
        if (!PLATFORMS.contains(normalized)) {
            throw new IllegalArgumentException("Invalid platform");
        }
        return normalized;
    }

    private String requireManagedPlatform(String platform) {
        return requirePlatform(platform);
    }

    private String requireTopic(String topic) {
        String normalized = topic == null ? "" : topic.trim();
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid topic");
        }
        return normalized;
    }

    private Map<String, Object> findAccount(String platform, String topic) {
        List<Map<String, Object>> rows = repository.query(
                accountBaseSelectSql("AND ol.platform = ? AND ol.topic = ?"),
                (rs, rowNum) -> mapAccount(rs),
                platform,
                topic
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    private Map<String, Object> updateGenericState(String platform, String topic, String column, boolean value) {
        String normalizedPlatform = requireManagedPlatform(platform);
        String normalizedTopic = requireTopic(topic);
        int updated = repository.update(
                "UPDATE uploader_account SET " + column + " = ?, updated_at = NOW() WHERE platform = ? AND topic = ? AND is_deprecated = 0",
                value,
                normalizedPlatform,
                normalizedTopic
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        return findAccount(normalizedPlatform, normalizedTopic);
    }
}
