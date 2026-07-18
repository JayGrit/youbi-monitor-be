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
import java.util.concurrent.ConcurrentHashMap;

abstract class MonitorRepositorySqlSupport {
    protected static final List<StageDefinition> STAGES = List.of(
            new StageDefinition("downloader", "下载", "downloader_status", "downloader_started_at", "downloader_completed_at", "downloader_error"),
            new StageDefinition("publisher", "发布准备", "publisher_status", "publisher_started_at", "publisher_completed_at", "publisher_error"),
            new StageDefinition("demucs", "人声分离", "demucs_status", "demucs_started_at", "demucs_completed_at", "demucs_error"),
            new StageDefinition("whisper", "语音识别", "whisper_status", "whisper_started_at", "whisper_completed_at", "whisper_error"),
            new StageDefinition("translator", "翻译", "translator_status", "translator_started_at", "translator_completed_at", "translator_error"),
            new StageDefinition("speaker", "配音", "speaker_status", "speaker_started_at", "speaker_completed_at", "speaker_error"),
            new StageDefinition("asseter", "素材加工", "asseter_status", "asseter_started_at", "asseter_completed_at", "asseter_error"),
            new StageDefinition("combiner", "音视频合成", "combiner_status", "combiner_started_at", "combiner_completed_at", "combiner_error"),
            new StageDefinition("uploader", "上传", "uploader_status", "uploader_started_at", "uploader_completed_at", "uploader_error")
    );
    protected static final List<RetryStage> RETRY_STAGES = List.of(
            new RetryStage("downloader", "downloader"),
            new RetryStage("publisher", "publisher"),
            new RetryStage("demucs", "demucs"),
            new RetryStage("whisper", "whisper"),
            new RetryStage("translator", "translator"),
            new RetryStage("speaker", "speaker"),
            new RetryStage("combiner", "combiner"),
            new RetryStage("uploader", "uploader")
    );
    protected static final List<String> RESET_CHILD_TABLES = List.of(
            "speaker_segment",
            "translator_segment",
            "translator_api_task",
            "translator_jobs",
            "whisper_word_timestamp",
            "whisper_asr_segment"
    );
    protected static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_task",
            "douyin", "uploader_task",
            "xiaohongshu", "uploader_task",
            "shipinhao", "uploader_task",
            "kuaishou", "uploader_task",
            "jinritoutiao", "uploader_task",
            "youtube", "uploader_task",
            "x", "uploader_task"
    );
    protected static final Map<String, String> UPLOADER_ACCOUNT_TABLES = Map.of(
            "bilibili", "uploader_account_bilibili",
            "douyin", "uploader_account_douyin",
            "xiaohongshu", "uploader_account_xiaohongshu",
            "shipinhao", "uploader_account_shipinhao",
            "kuaishou", "uploader_account_kuaishou",
            "jinritoutiao", "uploader_account_jinritoutiao",
            "youtube", "uploader_account_youtube",
            "x", "uploader_account_x"
    );
    protected static final String UNIFIED_UPLOADER_ACCOUNT_TABLE = "uploader_account";
    protected static final List<String> PRESERVED_VIDEO_INFO_COLUMNS = List.of(
            "task_id",
            "source_url",
            "submitter_video_id",
            "topic",
            "task_type",
            "has_background_audio",
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
    private final Map<String, Boolean> tableExistsCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> columnExistsCache = new ConcurrentHashMap<>();

    protected MonitorRepositorySqlSupport(MonitorRepository repository) {
        this.repository = repository;
    }

    public boolean tableExists(String table) {
        return tableExistsCache.computeIfAbsent(table, key -> {
            Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                """, Integer.class, key);
            return count != null && count > 0;
        });
    }

    protected boolean columnExists(String table, String column) {
        String cacheKey = table + "\u0000" + column;
        return columnExistsCache.computeIfAbsent(cacheKey, key -> {
            Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """, Integer.class, table, column);
            return count != null && count > 0;
        });
    }

    protected void invalidateSchemaCapability(String table, String column) {
        tableExistsCache.remove(table);
        if (column != null) {
            columnExistsCache.remove(table + "\u0000" + column);
        }
    }

    protected void ensureOperatorColumn(String table) {
    }

    protected void dropColumnIfExists(String table, String column) {
        if (tableExists(table) && columnExists(table, column)) {
            repository.update("ALTER TABLE " + quotedIdentifier(table) + " DROP COLUMN " + quotedIdentifier(column));
            invalidateSchemaCapability(table, column);
        }
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
        } else if ("yt".equals(normalized) || "油管".equals(normalized)) {
            normalized = "youtube";
        }
        if (!UPLOADER_TASK_TABLES.containsKey(normalized)) {
            throw new IllegalArgumentException("Unsupported upload platform: " + platform);
        }
        return normalized;
    }

    protected String successfulUploadUnion(String excludedPlatform) {
        return UPLOADER_TASK_TABLES.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(excludedPlatform))
                .filter(entry -> tableExists(entry.getValue()))
                .map(entry -> "SELECT task_id, platform FROM " + quotedIdentifier(entry.getValue()) + " WHERE platform = '" + entry.getKey() + "' AND status = 'success'")
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
                    NULLIF(final_video.alidrive_remote_path, ''),
                    NULLIF(final_video.alidrive_file_id, '')
                  )), 'adrive://'),
                  ''
                )
                """;
    }

    protected void applyStagedPipelineFailure(String taskId, String oldTaskStatus) {
        return;
    }

    protected void applyStagedPipelineRetry(String taskId) {
        return;
    }

    protected int reconcileUploaderAccountStagedCounts() {
        return 0;
    }

    private String stagedTopicForTask(String taskId) {
        if (!tableExists(UNIFIED_UPLOADER_ACCOUNT_TABLE)
                || !tableExists("downloader_submission")
                || !tableExists("task")) {
            return "";
        }
        String uploadExistsSql = uploadSubmissionExistsSql("submission");
        List<String> topics = repository.queryForList("""
                SELECT submission.topic
                FROM downloader_submission submission
                JOIN task task ON task.id = submission.task_id
                WHERE submission.task_id = ?
                  AND submission.status = 'success'
                  AND NULLIF(submission.topic, '') IS NOT NULL
                  AND NOT (%s)
                LIMIT 1
                FOR UPDATE
                """.formatted(uploadExistsSql), String.class, taskId);
        return topics.isEmpty() ? "" : text(topics.get(0));
    }

    private String uploadSubmissionExistsSql(String submissionAlias) {
        List<String> existsTerms = new ArrayList<>();
        for (String table : UPLOADER_TASK_TABLES.values()) {
            if (!tableExists(table)) {
                continue;
            }
            existsTerms.add("""
                    EXISTS (
                        SELECT 1
                        FROM %s upload_submission
                        WHERE upload_submission.task_id = %s.task_id
                          AND upload_submission.topic = %s.topic
                    )
                    """.formatted(quotedIdentifier(table), submissionAlias, submissionAlias));
        }
        return existsTerms.isEmpty() ? "0" : String.join(" OR ", existsTerms);
    }

    private void ensureUnifiedUploaderStagingColumns() {
        return;
    }

    private void ensureUploaderAccountRow(String platform, String topic) {
        repository.update("""
                INSERT INTO %s (
                    platform, topic, source_table, is_enabled, is_available, updated_at
                )
                VALUES (?, ?, ?, 1, 1, NOW())
                ON DUPLICATE KEY UPDATE updated_at = updated_at
                """.formatted(quotedIdentifier(UNIFIED_UPLOADER_ACCOUNT_TABLE)),
                platform,
                topic,
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

    private record UploadAccountStatusDeltaKey(String topic, String oldStatus, String newStatus) {
    }
}
