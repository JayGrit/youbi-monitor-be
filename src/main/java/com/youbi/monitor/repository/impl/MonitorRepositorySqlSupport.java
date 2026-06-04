package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.MonitorRepository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class MonitorRepositorySqlSupport {
    protected static final List<StageDefinition> STAGES = List.of(
            new StageDefinition("downloader", "下载", "downloader_status", "downloader_started_at", "downloader_completed_at", "downloader_error"),
            new StageDefinition("demucs", "人声分离", "demucs_status", "demucs_started_at", "demucs_completed_at", "demucs_error"),
            new StageDefinition("whisper", "语音识别", "whisper_status", "whisper_started_at", "whisper_completed_at", "whisper_error"),
            new StageDefinition("translator", "翻译", "translator_status", "translator_started_at", "translator_completed_at", "translator_error"),
            new StageDefinition("speaker", "配音", "speaker_status", "speaker_started_at", "speaker_completed_at", "speaker_error"),
            new StageDefinition("combiner", "音视频合成", "combiner_status", "combiner_started_at", "combiner_completed_at", "combiner_error"),
            new StageDefinition("uploader", "上传", "uploader_status", "uploader_started_at", "uploader_completed_at", "uploader_error")
    );
    protected static final List<RetryStage> RETRY_STAGES = List.of(
            new RetryStage("downloader", "yd_downloader"),
            new RetryStage("demucs", "yd_demucs"),
            new RetryStage("whisper", "yd_whisper"),
            new RetryStage("translator", "yd_translator"),
            new RetryStage("speaker", "yd_speaker"),
            new RetryStage("combiner", "yd_combiner"),
            new RetryStage("uploader", "yd_uploader")
    );
    protected static final List<String> RESET_CHILD_TABLES = List.of(
            "yd_speaker_segment",
            "yd_translator_api_task",
            "whisper_word_timestamp",
            "yd_asr_segment"
    );
    protected static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_task_bilibili",
            "douyin", "uploader_task_douyin",
            "xiaohongshu", "uploader_task_xiaohongshu",
            "shipinhao", "uploader_task_shipinhao",
            "kuaishou", "uploader_task_kuaishou",
            "jinritoutiao", "uploader_task_jinritoutiao"
    );
    protected static final Map<String, String> UPLOADER_ACCOUNT_TABLES = Map.of(
            "bilibili", "uploader_account_bilibili",
            "douyin", "uploader_account_douyin",
            "xiaohongshu", "uploader_account_xiaohongshu",
            "shipinhao", "uploader_account_shipinhao",
            "kuaishou", "uploader_account_kuaishou",
            "jinritoutiao", "uploader_account_jinritoutiao"
    );
    protected static final String UNIFIED_UPLOADER_ACCOUNT_TABLE = "uploader_account";
    protected static final List<String> PRESERVED_VIDEO_INFO_COLUMNS = List.of(
            "task_id",
            "source_url",
            "source_platform",
            "type",
            "source_uploader",
            "need_subtitle",
            "need_dubbing",
            "need_separation",
            "source_language",
            "target_language",
            "created_at",
            "updated_at"
    );
    protected static final List<String> SYSTEM_STAGE_COLUMNS = List.of(
            "task_id",
            "status",
            "started_at",
            "completed_at",
            "error_message",
            "operator",
            "created_at",
            "updated_at"
    );

    protected final MonitorRepository repository;

    protected MonitorRepositorySqlSupport(MonitorRepository repository) {
        this.repository = repository;
    }

    public boolean tableExists(String table) {
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                """, Integer.class, table);
        return count != null && count > 0;
    }

    protected boolean columnExists(String table, String column) {
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """, Integer.class, table, column);
        return count != null && count > 0;
    }

    protected void ensureOperatorColumn(String table) {
    }

    protected static String quotedIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    protected static String placeholders(int count) {
        return "?,".repeat(Math.max(0, count)).replaceFirst(",$", "");
    }

    protected static LocalDateTime timestamp(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    protected static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    protected static long elapsedSeconds(LocalDateTime startedAt, LocalDateTime completedAt, LocalDateTime now) {
        if (startedAt == null) {
            return 0;
        }
        LocalDateTime end = completedAt == null ? now : completedAt;
        return Math.max(0, Duration.between(startedAt, end).getSeconds());
    }

    protected static String text(String value) {
        return value == null ? "" : value.trim();
    }

    protected static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    protected static LocalDateTime localDateTime(Object value) {
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime();
        }
        return null;
    }

    protected static boolean boolValue(Object value, boolean fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        String text = String.valueOf(value).trim().toLowerCase();
        if (text.isBlank()) {
            return fallback;
        }
        return !Set.of("0", "false", "no", "off").contains(text);
    }

    protected static String defaultLanguage(Object value, String fallback) {
        String normalized = text(value == null ? null : String.valueOf(value));
        return normalized.isBlank() ? fallback : normalized;
    }

    protected static List<String> splitCsv(String value) {
        String normalized = text(value);
        if (normalized.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(normalized.split(","))
                .map(MonitorRepositorySqlSupport::text)
                .filter(item -> !item.isBlank())
                .distinct()
                .toList();
    }

    protected static String normalizeUploadPlatform(String platform) {
        String normalized = text(platform).toLowerCase();
        if ("bili".equals(normalized)) {
            normalized = "bilibili";
        } else if ("xhs".equals(normalized) || "red".equals(normalized)) {
            normalized = "xiaohongshu";
        } else if ("dy".equals(normalized)) {
            normalized = "douyin";
        } else if ("sph".equals(normalized) || "channels".equals(normalized) || "wx_channels".equals(normalized) || "wechat_channels".equals(normalized) || "weixin-channels".equals(normalized) || "视频号".equals(normalized)) {
            normalized = "shipinhao";
        } else if ("ks".equals(normalized) || "kwai".equals(normalized) || "快手".equals(normalized)) {
            normalized = "kuaishou";
        } else if ("toutiao".equals(normalized) || "tt".equals(normalized) || "今日头条".equals(normalized)) {
            normalized = "jinritoutiao";
        }
        if (!UPLOADER_TASK_TABLES.containsKey(normalized)) {
            throw new IllegalArgumentException("Unsupported upload platform: " + platform);
        }
        return normalized;
    }

    protected static String successfulUploadUnion(String excludedPlatform) {
        return UPLOADER_TASK_TABLES.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(excludedPlatform))
                .map(entry -> "SELECT task_id, '" + entry.getKey() + "' platform FROM " + quotedIdentifier(entry.getValue()) + " WHERE status = 'success'")
                .reduce((left, right) -> left + "\nUNION ALL\n" + right)
                .orElse("SELECT NULL task_id, NULL platform WHERE FALSE");
    }

    protected static String uploadStatusColumn(String platform) {
        return normalizeUploadPlatform(platform) + "_upload_status";
    }

    protected static String finalVideoRefSql() {
        return """
                COALESCE(
                  NULLIF(video_info.final_video_url, ''),
                  NULLIF(CONCAT('adrive://', COALESCE(
                    NULLIF(uploader.final_video_alidrive_remote_path, ''),
                    NULLIF(uploader.alidrive_final_video_remote_path, ''),
                    NULLIF(uploader.final_video_alidrive_file_id, ''),
                    NULLIF(uploader.alidrive_final_video_file_id, '')
                  )), 'adrive://'),
                  ''
                )
                """;
    }

    protected void applyUploaderAccountStatusChanges(String platform, List<UploadAccountStatusChange> changes) {
        String normalizedPlatform = normalizeUploadPlatform(platform);
        if (changes == null || changes.isEmpty() || !tableExists(UNIFIED_UPLOADER_ACCOUNT_TABLE)) {
            return;
        }
        boolean hasFailedUploadCount = columnExists(UNIFIED_UPLOADER_ACCOUNT_TABLE, "failed_upload_count");
        Map<UploadAccountStatusDeltaKey, Integer> counts = new LinkedHashMap<>();
        for (UploadAccountStatusChange change : changes) {
            String accountKey = text(change.accountKey());
            if (accountKey.isBlank()) {
                continue;
            }
            UploadAccountStatusDeltaKey key = new UploadAccountStatusDeltaKey(
                    accountKey,
                    normalizeStatus(change.oldStatus()),
                    normalizeStatus(change.newStatus())
            );
            counts.put(key, counts.getOrDefault(key, 0) + 1);
        }
        for (Map.Entry<UploadAccountStatusDeltaKey, Integer> entry : counts.entrySet()) {
            UploadAccountStatusDeltaKey key = entry.getKey();
            int count = entry.getValue();
            ensureUploaderAccountRow(normalizedPlatform, key.accountKey());
            int runningDelta = (runningContribution(key.newStatus()) - runningContribution(key.oldStatus())) * count;
            int failedDelta = (failedContribution(key.newStatus()) - failedContribution(key.oldStatus())) * count;
            int todayDelta = (successContribution(key.newStatus()) - successContribution(key.oldStatus())) * count;
            int waitingDelta = (runningContribution(key.newStatus()) - runningContribution(key.oldStatus())) * count;
            int readyDelta = (readyContribution(key.newStatus()) - readyContribution(key.oldStatus())) * count;
            if (runningDelta == 0 && failedDelta == 0 && todayDelta == 0 && waitingDelta == 0 && readyDelta == 0) {
                continue;
            }

            List<Object> args = new ArrayList<>();
            args.add(runningDelta);
            if (hasFailedUploadCount) {
                args.add(failedDelta);
            }
            args.add(todayDelta);
            args.add(waitingDelta);
            args.add(readyDelta);
            args.add(normalizedPlatform);
            args.add(key.accountKey());
            repository.update("""
                    UPDATE %s
                    SET upload_running_count = GREATEST(0, upload_running_count + ?),
                        %s
                        today_upload_count = GREATEST(0, today_upload_count + ?),
                        cooldown_waiting_count = GREATEST(
                            0,
                            cooldown_waiting_count
                            + ?
                            + CASE WHEN next_upload_allowed_at > NOW() THEN ? ELSE 0 END
                        ),
                        metrics_updated_at = NOW(),
                        updated_at = NOW()
                    WHERE platform = ? AND account_key = ?
                    """.formatted(
                            quotedIdentifier(UNIFIED_UPLOADER_ACCOUNT_TABLE),
                            hasFailedUploadCount
                                    ? "failed_upload_count = GREATEST(0, failed_upload_count + ?),"
                                    : ""
                    ),
                    args.toArray()
            );
        }
    }

    private void ensureUploaderAccountRow(String platform, String accountKey) {
        repository.update("""
                INSERT INTO %s (
                    platform, account_key, source_table, is_enabled, is_available, updated_at
                )
                VALUES (?, ?, ?, 1, 1, NOW())
                ON DUPLICATE KEY UPDATE updated_at = updated_at
                """.formatted(quotedIdentifier(UNIFIED_UPLOADER_ACCOUNT_TABLE)),
                platform,
                accountKey,
                UPLOADER_ACCOUNT_TABLES.get(platform)
        );
    }

    private static String normalizeStatus(String status) {
        return text(status).toLowerCase();
    }

    private static int runningContribution(String status) {
        return "running".equals(status) ? 1 : 0;
    }

    private static int failedContribution(String status) {
        return "failed".equals(status) ? 1 : 0;
    }

    private static int successContribution(String status) {
        return "success".equals(status) ? 1 : 0;
    }

    private static int readyContribution(String status) {
        return "ready".equals(status) ? 1 : 0;
    }

    protected record RetryStage(String key, String table) {
    }

    protected record StageDefinition(
            String key,
            String label,
            String statusColumn,
            String startedAtColumn,
            String completedAtColumn,
            String errorColumn
    ) {
    }

    protected record UploadBackfillInsertRow(
            String taskId,
            String title,
            String finalVideoUrl,
            String coverUrl,
            String description,
            String tags
    ) {
    }

    protected record UploadAccountStatusChange(String accountKey, String oldStatus, String newStatus) {
    }

    private record UploadAccountStatusDeltaKey(String accountKey, String oldStatus, String newStatus) {
    }
}
