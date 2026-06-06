package com.youbi.monitor.service;

import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
            "jinritoutiao"
    );

    private final MonitorRepository repository;

    public AccountOverviewService(MonitorRepository repository) {
        this.repository = repository;
    }

    public List<String> types() {
        return repository.queryForList("""
                SELECT DISTINCT account_key
                FROM uploader_account
                WHERE account_key IS NOT NULL AND TRIM(account_key) <> ''
                ORDER BY account_key
                """, String.class);
    }

    public Map<String, List<Map<String, Object>>> overview() {
        ensureQuietTimeColumns();
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        PLATFORMS.forEach(platform -> result.put(platform, new ArrayList<>()));
        repository.query(accountSelectSql("") + """
                ORDER BY FIELD(ua.platform, 'douyin', 'xiaohongshu', 'bilibili', 'shipinhao', 'kuaishou', 'jinritoutiao'), ua.account_key
                """,
                rs -> result.computeIfAbsent(rs.getString("platform"), ignored -> new ArrayList<>()).add(mapAccount(rs))
        );
        return result;
    }

    public BackupperStatus latestBackupperStatus() {
        if (!tableExists("backupper_status")) {
            return null;
        }
        List<BackupperStatus> rows = repository.query(
                """
                SELECT id, host, device, mount_point, total_gb, used_gb, available_gb, used_percent, total_label, created_at
                FROM backupper_status
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (rs, rowNum) -> new BackupperStatus(
                        rs.getLong("id"),
                        rs.getString("host"),
                        rs.getString("device"),
                        rs.getString("mount_point"),
                        rs.getBigDecimal("total_gb"),
                        rs.getBigDecimal("used_gb"),
                        rs.getBigDecimal("available_gb"),
                        rs.getBigDecimal("used_percent"),
                        formatBackupperStatusText(rs.getBigDecimal("used_gb"), rs.getString("total_label")),
                        toLocalDateTime(rs.getTimestamp("created_at"))
                )
        );
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Object> updateNextUploadAllowedAt(String platform, String accountKey, LocalDateTime nextUploadAllowedAt) {
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedAccountKey = accountKey == null ? "" : accountKey.trim();
        if (!PLATFORMS.contains(normalizedPlatform) || normalizedAccountKey.isBlank()) {
            throw new IllegalArgumentException("Invalid account");
        }
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET next_upload_allowed_at = ?, updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                nextUploadAllowedAt,
                normalizedPlatform,
                normalizedAccountKey
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        List<Map<String, Object>> rows = repository.query(
                accountSelectSql("AND ua.platform = ? AND ua.account_key = ?"),
                (rs, rowNum) -> mapAccount(rs),
                normalizedPlatform,
                normalizedAccountKey
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    public Map<String, Object> updateQuietTime(String platform, String accountKey, LocalTime startTime, LocalTime endTime) {
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedAccountKey = accountKey == null ? "" : accountKey.trim();
        if (!PLATFORMS.contains(normalizedPlatform) || normalizedAccountKey.isBlank()) {
            throw new IllegalArgumentException("Invalid account");
        }
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("Quiet time is required");
        }
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
                normalizedPlatform,
                normalizedAccountKey
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        List<Map<String, Object>> rows = repository.query(
                accountSelectSql("AND ua.platform = ? AND ua.account_key = ?"),
                (rs, rowNum) -> mapAccount(rs),
                normalizedPlatform,
                normalizedAccountKey
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    public Map<String, Object> updateDownloaderMaxStagedCount(String platform, String accountKey, Integer maxStagedCount) {
        String normalizedPlatform = normalizePlatform(platform);
        String normalizedAccountKey = accountKey == null ? "" : accountKey.trim();
        int normalizedMax = maxStagedCount == null ? 5 : maxStagedCount;
        if (!PLATFORMS.contains(normalizedPlatform) || normalizedAccountKey.isBlank()) {
            throw new IllegalArgumentException("Invalid account");
        }
        if (normalizedMax < 0 || normalizedMax > 100) {
            throw new IllegalArgumentException("Invalid downloader max staged count: " + normalizedMax);
        }
        ensureDownloaderStagingColumns();
        int updated = repository.update(
                """
                UPDATE uploader_account
                SET downloader_max_staged_count = ?, updated_at = NOW()
                WHERE platform = ? AND account_key = ?
                """,
                normalizedMax,
                normalizedPlatform,
                normalizedAccountKey
        );
        if (updated != 1) {
            throw new IllegalArgumentException("Account not found");
        }
        List<Map<String, Object>> rows = repository.query(
                accountSelectSql("AND ua.platform = ? AND ua.account_key = ?"),
                (rs, rowNum) -> mapAccount(rs),
                normalizedPlatform,
                normalizedAccountKey
        );
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Account not found");
        }
        return rows.get(0);
    }

    private String accountSelectSql(String extraWhere) {
        String failedCountSql = columnExists("uploader_account", "failed_upload_count")
                ? "ua.failed_upload_count"
                : "0 failed_upload_count";
        boolean hasRunningTaskId = columnExists("uploader_account", "upload_running_task_id");
        String runningTaskIdSql = hasRunningTaskId
                ? "NULLIF(ua.upload_running_task_id, '') AS upload_running_task_id"
                : "NULL AS upload_running_task_id";
        String runningCountSql = hasRunningTaskId
                ? "CASE WHEN NULLIF(ua.upload_running_task_id, '') IS NULL THEN 0 ELSE 1 END AS upload_running_count"
                : "ua.upload_running_count";
        String downloaderMaxStagedCountSql = columnExists("uploader_account", "downloader_max_staged_count")
                ? "ua.downloader_max_staged_count"
                : "5 downloader_max_staged_count";
        String downloaderPendingCountSql = columnExists("uploader_account", "downloader_pending_count")
                ? "ua.downloader_pending_count"
                : "0 downloader_pending_count";
        String stagedRunningCountSql = columnExists("uploader_account", "staged_running_count")
                ? "ua.staged_running_count"
                : "0 staged_running_count";
        String quietStartSql = columnExists("uploader_account", "upload_quiet_start_time")
                ? "ua.upload_quiet_start_time"
                : "TIME('01:00:00') upload_quiet_start_time";
        String quietEndSql = columnExists("uploader_account", "upload_quiet_end_time")
                ? "ua.upload_quiet_end_time"
                : "TIME('07:00:00') upload_quiet_end_time";
        return ("""
                SELECT ua.platform,
                       ua.account_key,
                       ua.last_upload_at,
                       ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds,
                       ua.upload_cooldown_max_seconds,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       ua.today_upload_count,
                       ua.cooldown_waiting_count,
                       %s,
                       %s,
                       %s,
                       ua.is_enabled,
                       ua.is_available,
                       b.mid,
                       b.uname,
                       CASE ua.platform
                         WHEN 'douyin' THEN d.user_id
                         WHEN 'xiaohongshu' THEN x.user_id
                         WHEN 'shipinhao' THEN s.user_id
                         WHEN 'kuaishou' THEN k.user_id
                         WHEN 'jinritoutiao' THEN j.user_id
                         ELSE NULL
                       END AS user_id,
                       CASE ua.platform
                         WHEN 'douyin' THEN d.nickname
                         WHEN 'xiaohongshu' THEN x.nickname
                         WHEN 'shipinhao' THEN s.nickname
                         WHEN 'kuaishou' THEN k.nickname
                         WHEN 'jinritoutiao' THEN j.nickname
                         ELSE NULL
                       END AS nickname,
                       CASE ua.platform
                         WHEN 'bilibili' THEN b.login_info_json
                         WHEN 'douyin' THEN d.storage_state_json
                         WHEN 'xiaohongshu' THEN x.storage_state_json
                         WHEN 'shipinhao' THEN s.storage_state_json
                         WHEN 'kuaishou' THEN k.storage_state_json
                         WHEN 'jinritoutiao' THEN j.storage_state_json
                         ELSE NULL
                       END AS storage_json,
                       CASE ua.platform
                         WHEN 'bilibili' THEN b.updated_at
                         WHEN 'douyin' THEN d.updated_at
                         WHEN 'xiaohongshu' THEN x.updated_at
                         WHEN 'shipinhao' THEN s.updated_at
                         WHEN 'kuaishou' THEN k.updated_at
                         WHEN 'jinritoutiao' THEN j.updated_at
                         ELSE NULL
                       END AS cookie_updated_at,
                       CASE ua.platform
                         WHEN 'bilibili' THEN b.display_name
                         WHEN 'douyin' THEN d.display_name
                         WHEN 'xiaohongshu' THEN x.display_name
                         WHEN 'shipinhao' THEN s.display_name
                         WHEN 'kuaishou' THEN k.display_name
                         WHEN 'jinritoutiao' THEN j.display_name
                         ELSE NULL
                       END AS display_name,
                       CASE ua.platform
                         WHEN 'bilibili' THEN b.avatar_url
                         WHEN 'douyin' THEN d.avatar_url
                         WHEN 'xiaohongshu' THEN x.avatar_url
                         WHEN 'shipinhao' THEN s.avatar_url
                         WHEN 'kuaishou' THEN k.avatar_url
                         WHEN 'jinritoutiao' THEN j.avatar_url
                         ELSE NULL
                       END AS avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_bilibili b
                       ON ua.platform = 'bilibili' AND b.account_key = ua.account_key
                LEFT JOIN uploader_account_douyin d
                       ON ua.platform = 'douyin' AND d.account_key = ua.account_key
                LEFT JOIN uploader_account_xiaohongshu x
                       ON ua.platform = 'xiaohongshu' AND x.account_key = ua.account_key
                LEFT JOIN uploader_account_shipinhao s
                       ON ua.platform = 'shipinhao' AND s.account_key = ua.account_key
                LEFT JOIN uploader_account_kuaishou k
                       ON ua.platform = 'kuaishou' AND k.account_key = ua.account_key
                LEFT JOIN uploader_account_jinritoutiao j
                       ON ua.platform = 'jinritoutiao' AND j.account_key = ua.account_key
                WHERE ua.platform IN ('douyin', 'xiaohongshu', 'bilibili', 'shipinhao', 'kuaishou', 'jinritoutiao')
                %s
                """).formatted(quietStartSql, quietEndSql, downloaderMaxStagedCountSql, downloaderPendingCountSql, stagedRunningCountSql, runningTaskIdSql, runningCountSql, failedCountSql, extraWhere == null ? "" : extraWhere);
    }

    private Map<String, Object> mapAccount(ResultSet rs) throws java.sql.SQLException {
        String platform = rs.getString("platform");
        String storageJson = rs.getString("storage_json");
        boolean cookieExists = storageJson != null && !storageJson.isBlank();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("storage", "database");
        row.put("accountKey", rs.getString("account_key"));
        row.put("cookieExists", cookieExists);
        row.put("cookieSizeBytes", cookieExists ? storageJson.getBytes(StandardCharsets.UTF_8).length : 0);
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
        row.put("downloaderPendingCount", rs.getInt("downloader_pending_count"));
        row.put("stagedRunningCount", rs.getInt("staged_running_count"));
        row.put("todayUploadCount", rs.getInt("today_upload_count"));
        row.put("cooldownWaitingCount", rs.getInt("cooldown_waiting_count"));
        row.put("uploadRunningTaskId", rs.getString("upload_running_task_id"));
        row.put("uploadRunningCount", rs.getInt("upload_running_count"));
        row.put("failedUploadCount", rs.getInt("failed_upload_count"));
        row.put("enabled", rs.getBoolean("is_enabled"));
        row.put("available", nullableBoolean(rs, "is_available"));
        row.put("valid", null);
        row.put("message", cookieExists ? "已保存" : "未登录");
        row.put("raw", Map.of());
        row.put("displayName", rs.getString("display_name"));
        row.put("avatarUrl", rs.getString("avatar_url"));
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

    private void ensureDownloaderStagingColumns() {
        if (!tableExists("uploader_account")) {
            return;
        }
        if (!columnExists("uploader_account", "downloader_max_staged_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN downloader_max_staged_count INT NOT NULL DEFAULT 5
                    """
            );
        }
        if (!columnExists("uploader_account", "downloader_pending_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN downloader_pending_count INT NOT NULL DEFAULT 0
                    """
            );
        }
        if (!columnExists("uploader_account", "staged_running_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN staged_running_count INT NOT NULL DEFAULT 0
                    """
            );
        }
    }

    private void ensureQuietTimeColumns() {
        if (!tableExists("uploader_account")) {
            return;
        }
        if (!columnExists("uploader_account", "upload_quiet_start_time")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN upload_quiet_start_time TIME NOT NULL DEFAULT '01:00:00'
                    """
            );
        }
        if (!columnExists("uploader_account", "upload_quiet_end_time")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN upload_quiet_end_time TIME NOT NULL DEFAULT '07:00:00'
                    """
            );
        }
    }

    private static String formatBackupperStatusText(BigDecimal usedGb, String totalLabel) {
        if (usedGb == null) {
            return "";
        }
        String total = totalLabel == null ? "" : totalLabel.trim();
        if (total.isBlank()) {
            return oneDecimal(usedGb) + "G";
        }
        return oneDecimal(usedGb) + "G/" + total;
    }

    private static String oneDecimal(BigDecimal value) {
        return value.setScale(1, java.math.RoundingMode.HALF_UP).toPlainString();
    }

    private static Boolean nullableBoolean(ResultSet rs, String column) throws java.sql.SQLException {
        boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : value;
    }

    private static String normalizePlatform(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }
}
