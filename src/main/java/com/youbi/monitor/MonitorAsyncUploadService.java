package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class MonitorAsyncUploadService {
    private static final Logger log = LoggerFactory.getLogger(MonitorAsyncUploadService.class);
    private static final String TABLE = "monitor_upload_task";
    private static final int UPLOAD_TASK_TIMEOUT_SECONDS = 75 * 60;
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final BilibiliUploadService bilibiliUploadService;
    private final BilibiliPlaywrightUploadService bilibiliPlaywrightUploadService;
    private final XiaohongshuUploadService xiaohongshuUploadService;
    private final DouyinUploadService douyinUploadService;
    private final ShipinhaoUploadService shipinhaoUploadService;
    private final KuaishouUploadService kuaishouUploadService;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "monitor-upload-task");
        thread.setDaemon(true);
        return thread;
    });

    public MonitorAsyncUploadService(
            BilibiliUploadService bilibiliUploadService,
            BilibiliPlaywrightUploadService bilibiliPlaywrightUploadService,
            XiaohongshuUploadService xiaohongshuUploadService,
            DouyinUploadService douyinUploadService,
            ShipinhaoUploadService shipinhaoUploadService,
            KuaishouUploadService kuaishouUploadService,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper
    ) {
        this.bilibiliUploadService = bilibiliUploadService;
        this.bilibiliPlaywrightUploadService = bilibiliPlaywrightUploadService;
        this.xiaohongshuUploadService = xiaohongshuUploadService;
        this.douyinUploadService = douyinUploadService;
        this.shipinhaoUploadService = shipinhaoUploadService;
        this.kuaishouUploadService = kuaishouUploadService;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void ensureSchema() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS monitor_upload_task (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    upload_task_id VARCHAR(64) NOT NULL,
                    platform VARCHAR(32) NOT NULL,
                    upstream_task_id VARCHAR(64) NULL,
                    account_key VARCHAR(128) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    request_json MEDIUMTEXT NULL,
                    result_json MEDIUMTEXT NULL,
                    error_code VARCHAR(64) NULL,
                    error_message TEXT NULL,
                    video_url TEXT NULL,
                    started_at DATETIME NULL,
                    completed_at DATETIME NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_monitor_upload_task (upload_task_id),
                    KEY idx_monitor_upload_running (status),
                    KEY idx_monitor_upload_platform (platform, upstream_task_id, account_key)
                )
                """);
    }

    public synchronized MonitorUploadTaskResponse submit(String platform, Object request) {
        ensureSchema();
        failStaleRunningTasks();
        long running = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE status IN ('accepted', 'running')",
                Long.class
        );
        String normalizedPlatform = text(platform).toLowerCase();
        if (running > 0) {
            return new MonitorUploadTaskResponse(false, null, normalizedPlatform, taskId(request), accountKey(request),
                    "rejected", false, "已有上传任务正在执行", "UPLOAD_RUNNING", "已有上传任务正在执行",
                    null, requestVideoUrl(request), null, null, null, null, Map.of(), null, null);
        }

        String uploadTaskId = UUID.randomUUID().toString();
        jdbcTemplate.update(
                """
                INSERT INTO monitor_upload_task (
                    upload_task_id, platform, upstream_task_id, account_key, status,
                    request_json, video_url, started_at
                )
                VALUES (?, ?, NULLIF(?, ''), ?, 'accepted', ?, ?, NOW())
                """,
                uploadTaskId,
                normalizedPlatform,
                taskId(request),
                defaultText(accountKey(request), "default"),
                toJson(request),
                requestVideoUrl(request)
        );
        executor.submit(() -> execute(uploadTaskId, normalizedPlatform, request));
        return status(uploadTaskId);
    }

    public MonitorUploadTaskResponse status(String uploadTaskId) {
        ensureSchema();
        failStaleRunningTasks();
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT * FROM " + TABLE + " WHERE upload_task_id = ?",
                    (rs, rowNum) -> {
                        Map<String, Object> result = parseJsonMap(rs.getString("result_json"));
                        Map<String, Object> upload = uploadResult(result);
                        String status = rs.getString("status");
                        boolean success = "success".equals(status);
                        LocalDateTime startedAt = rs.getTimestamp("started_at") == null ? null : rs.getTimestamp("started_at").toLocalDateTime();
                        LocalDateTime completedAt = rs.getTimestamp("completed_at") == null ? null : rs.getTimestamp("completed_at").toLocalDateTime();
                        return new MonitorUploadTaskResponse(
                                true,
                                rs.getString("upload_task_id"),
                                rs.getString("platform"),
                                rs.getString("upstream_task_id"),
                                rs.getString("account_key"),
                                status,
                                success,
                                null,
                                rs.getString("error_code"),
                                firstText(rs.getString("error_message"), stringValue(upload.get("message")), status),
                                durationMs(result, startedAt, completedAt),
                                rs.getString("video_url"),
                                stringValue(upload.get("bvid")),
                                longValue(upload.get("aid")),
                                longValue(upload.get("accountUid")),
                                firstText(stringValue(upload.get("accountName")), stringValue(upload.get("accountKey"))),
                                rawResult(upload, result),
                                startedAt,
                                completedAt
                        );
                    },
                    uploadTaskId
            );
        } catch (EmptyResultDataAccessException exception) {
            return new MonitorUploadTaskResponse(false, uploadTaskId, null, null, null, "missing", false,
                    "上传任务不存在", "TASK_NOT_FOUND", "上传任务不存在", null, null, null, null, null, null, Map.of(), null, null);
        }
    }

    private void execute(String uploadTaskId, String platform, Object request) {
        long startedAt = System.currentTimeMillis();
        int started = jdbcTemplate.update(
                "UPDATE " + TABLE + " SET status = 'running', started_at = COALESCE(started_at, NOW()), error_message = NULL WHERE upload_task_id = ? AND status = 'accepted'",
                uploadTaskId
        );
        if (started == 0) {
            log.warn("Async upload skipped because task is no longer accepted uploadTaskId={} platform={} taskId={}",
                    uploadTaskId, platform, taskId(request));
            return;
        }
        try {
            Object result = upload(platform, request);
            Map<String, Object> resultMap = objectMapper.convertValue(result, MAP_TYPE);
            if (!Boolean.TRUE.equals(resultMap.get("success"))) {
                throw new IllegalStateException(stringValue(resultMap.get("message")));
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("durationMs", System.currentTimeMillis() - startedAt);
            payload.put("result", resultMap);
            int updated = jdbcTemplate.update(
                    "UPDATE " + TABLE + " SET status = 'success', result_json = ?, error_code = NULL, error_message = NULL, completed_at = NOW() WHERE upload_task_id = ? AND status = 'running'",
                    toJson(payload),
                    uploadTaskId
            );
            if (updated == 0) {
                log.warn("Async upload finished after task status changed uploadTaskId={} platform={} taskId={}",
                        uploadTaskId, platform, taskId(request));
            }
        } catch (Exception exception) {
            String message = firstText(exception.getMessage(), exception.getClass().getSimpleName());
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("durationMs", System.currentTimeMillis() - startedAt);
            payload.put("error", message);
            int updated = jdbcTemplate.update(
                    "UPDATE " + TABLE + " SET status = 'failed', result_json = ?, error_code = ?, error_message = ?, completed_at = NOW() WHERE upload_task_id = ? AND status = 'running'",
                    toJson(payload),
                    classifyErrorCode(message),
                    message,
                    uploadTaskId
            );
            if (updated == 0) {
                log.warn("Async upload failed after task status changed uploadTaskId={} platform={} taskId={} message={}",
                        uploadTaskId, platform, taskId(request), message);
                return;
            }
            log.warn("Async upload failed uploadTaskId={} platform={} taskId={} accountKey={} message={}",
                    uploadTaskId, platform, taskId(request), accountKey(request), message, exception);
        }
    }

    private Object upload(String platform, Object request) throws Exception {
        return switch (platform) {
            case "bilibili" -> uploadBilibili((BilibiliUploadRequest) request);
            case "xiaohongshu" -> xiaohongshuUploadService.upload((XiaohongshuUploadRequest) request);
            case "douyin" -> douyinUploadService.upload((DouyinUploadRequest) request);
            case "shipinhao" -> shipinhaoUploadService.upload((ShipinhaoUploadRequest) request);
            case "kuaishou" -> kuaishouUploadService.upload((KuaishouUploadRequest) request);
            default -> throw new IllegalArgumentException("Unsupported upload platform: " + platform);
        };
    }

    private BilibiliUploadResult uploadBilibili(BilibiliUploadRequest request) throws Exception {
        try {
            BilibiliUploadResult result = bilibiliUploadService.upload(request);
            if (result.success()) {
                return result;
            }
            if (shouldFallbackToPlaywright(result.message())) {
                return playwrightFallbackResult(request, result.message());
            }
            throw new IllegalStateException(result.message());
        } catch (Exception exception) {
            if (shouldFallbackToPlaywright(exception.getMessage())) {
                return playwrightFallbackResult(request, exception.getMessage());
            }
            throw exception;
        }
    }

    private BilibiliUploadResult playwrightFallbackResult(BilibiliUploadRequest request, String reason) throws Exception {
        log.warn("Bilibili API upload hit frequent/rate-limit error, falling back to Playwright: {}", reason);
        try {
            BilibiliUploadResult fallback = bilibiliPlaywrightUploadService.upload(request);
            Map<String, Object> raw = new LinkedHashMap<>();
            if (fallback.raw() != null) {
                raw.putAll(fallback.raw());
            }
            raw.put("fallbackFrom", "bilibili-api");
            raw.put("fallbackReason", reason);
            return new BilibiliUploadResult(
                    fallback.success(),
                    fallback.bvid(),
                    fallback.aid(),
                    fallback.accountUid(),
                    fallback.accountName(),
                    fallback.success() ? "Bilibili API 上传触发频繁限制，已降级 Playwright 上传成功" : fallback.message(),
                    raw
            );
        } catch (Exception fallbackException) {
            throw new IllegalStateException(
                    "Bilibili API 上传触发频繁限制，Playwright 降级也失败: " + fallbackException.getMessage(),
                    fallbackException
            );
        }
    }

    private boolean shouldFallbackToPlaywright(String message) {
        String text = message == null ? "" : message.toLowerCase();
        return text.contains("投稿频繁")
                || text.contains("操作频繁")
                || text.contains("频繁")
                || text.contains("风控")
                || text.contains("rate limit")
                || text.contains("too frequent");
    }

    private void failStaleRunningTasks() {
        jdbcTemplate.update("""
                UPDATE monitor_upload_task
                SET status = 'failed',
                    error_code = 'MONITOR_UPLOAD_TIMEOUT',
                    error_message = ?,
                    completed_at = NOW()
                WHERE status IN ('accepted', 'running')
                  AND started_at IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, started_at, NOW()) > ?
                """,
                "monitor upload task timed out after " + UPLOAD_TASK_TIMEOUT_SECONDS + "s",
                UPLOAD_TASK_TIMEOUT_SECONDS);
    }

    private String classifyErrorCode(String message) {
        String text = message == null ? "" : message.toLowerCase();
        if (text.contains("not logged in") || text.contains("login") || text.contains("登录") || text.contains("未登录")) {
            return "LOGIN_REQUIRED";
        }
        if (text.contains("playwright") && (text.contains("launch") || text.contains("browser") || text.contains("启动"))) {
            return "PLAYWRIGHT_START_FAILED";
        }
        if (text.contains("频繁") || text.contains("rate limit") || text.contains("风控")) {
            return "RATE_LIMITED";
        }
        return "UPLOAD_FAILED";
    }

    private Map<String, Object> uploadResult(Map<String, Object> result) {
        Object nested = result.get("result");
        if (nested instanceof Map<?, ?> map) {
            Map<String, Object> typed = new LinkedHashMap<>();
            map.forEach((key, value) -> typed.put(String.valueOf(key), value));
            return typed;
        }
        return Map.of();
    }

    private Map<String, Object> rawResult(Map<String, Object> upload, Map<String, Object> result) {
        Object raw = upload.get("raw");
        if (raw instanceof Map<?, ?> map) {
            Map<String, Object> typed = new LinkedHashMap<>();
            map.forEach((key, value) -> typed.put(String.valueOf(key), value));
            return typed;
        }
        return result;
    }

    private Long durationMs(Map<String, Object> result, LocalDateTime startedAt, LocalDateTime completedAt) {
        Long value = longValue(result.get("durationMs"));
        if (value != null) {
            return value;
        }
        return startedAt == null || completedAt == null ? null : Duration.between(startedAt, completedAt).toMillis();
    }

    private Map<String, Object> parseJsonMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (Exception exception) {
            return Map.of("rawResultJson", json);
        }
    }

    private String requestVideoUrl(Object request) {
        if (request instanceof BilibiliUploadRequest item) {
            return firstText(item.videoUrl(), item.minioUrl(), item.alidriveFileId(), item.alidriveRemotePath(), item.videoPath());
        }
        if (request instanceof XiaohongshuUploadRequest item) {
            return firstText(item.videoUrl(), item.minioUrl(), item.alidriveFileId(), item.alidriveRemotePath(), item.videoPath());
        }
        if (request instanceof DouyinUploadRequest item) {
            return firstText(item.videoUrl(), item.minioUrl(), item.alidriveFileId(), item.alidriveRemotePath(), item.videoPath());
        }
        if (request instanceof ShipinhaoUploadRequest item) {
            return firstText(item.videoUrl(), item.minioUrl(), item.alidriveFileId(), item.alidriveRemotePath(), item.videoPath());
        }
        if (request instanceof KuaishouUploadRequest item) {
            return firstText(item.videoUrl(), item.minioUrl(), item.alidriveFileId(), item.alidriveRemotePath(), item.videoPath());
        }
        return "";
    }

    private String taskId(Object request) {
        if (request instanceof BilibiliUploadRequest item) {
            return text(item.taskId());
        }
        if (request instanceof XiaohongshuUploadRequest item) {
            return text(item.taskId());
        }
        if (request instanceof DouyinUploadRequest item) {
            return text(item.taskId());
        }
        if (request instanceof ShipinhaoUploadRequest item) {
            return text(item.taskId());
        }
        if (request instanceof KuaishouUploadRequest item) {
            return text(item.taskId());
        }
        return "";
    }

    private String accountKey(Object request) {
        if (request instanceof BilibiliUploadRequest item) {
            return text(item.accountKey());
        }
        if (request instanceof XiaohongshuUploadRequest item) {
            return text(item.accountKey());
        }
        if (request instanceof DouyinUploadRequest item) {
            return text(item.accountKey());
        }
        if (request instanceof ShipinhaoUploadRequest item) {
            return text(item.accountKey());
        }
        if (request instanceof KuaishouUploadRequest item) {
            return text(item.accountKey());
        }
        return "";
    }

    private Long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            return "{}";
        }
    }

    private String firstText(String... values) {
        for (String value : values) {
            String text = text(value);
            if (!text.isBlank()) {
                return text;
            }
        }
        return "";
    }

    private String defaultText(String value, String fallback) {
        String text = text(value);
        return text.isBlank() ? fallback : text;
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }
}
