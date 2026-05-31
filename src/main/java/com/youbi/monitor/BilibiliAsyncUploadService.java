package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class BilibiliAsyncUploadService {
    private static final Logger log = LoggerFactory.getLogger(BilibiliAsyncUploadService.class);
    private static final String TABLE = "monitor_upload_task";
    private static final String PLATFORM = "bilibili";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final BilibiliUploadService uploadService;
    private final BilibiliPlaywrightUploadService playwrightUploadService;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "monitor-bilibili-upload");
        thread.setDaemon(true);
        return thread;
    });

    public BilibiliAsyncUploadService(
            BilibiliUploadService uploadService,
            BilibiliPlaywrightUploadService playwrightUploadService,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper
    ) {
        this.uploadService = uploadService;
        this.playwrightUploadService = playwrightUploadService;
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
                    KEY idx_monitor_upload_running (platform, status),
                    KEY idx_monitor_upload_upstream (platform, upstream_task_id, account_key)
                )
                """);
    }

    public synchronized MonitorUploadTaskResponse submit(BilibiliUploadRequest request) {
        ensureSchema();
        failStaleRunningTasks();
        long running = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE platform = ? AND status IN ('accepted', 'running')",
                Long.class,
                PLATFORM
        );
        if (running > 0) {
            return new MonitorUploadTaskResponse(
                    false,
                    null,
                    PLATFORM,
                    text(request.taskId()),
                    text(request.accountKey()),
                    "rejected",
                    false,
                    "已有 Bilibili 上传任务正在执行",
                    "UPLOAD_RUNNING",
                    "已有 Bilibili 上传任务正在执行",
                    null,
                    requestVideoUrl(request),
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    null,
                    null
            );
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
                PLATFORM,
                text(request.taskId()),
                text(request.accountKey()).isBlank() ? "default" : text(request.accountKey()),
                toJson(request),
                requestVideoUrl(request)
        );
        executor.submit(() -> execute(uploadTaskId, request));
        return status(uploadTaskId);
    }

    private void failStaleRunningTasks() {
        jdbcTemplate.update(
                """
                UPDATE monitor_upload_task
                SET status = 'failed',
                    error_code = 'MONITOR_UPLOAD_TIMEOUT',
                    error_message = 'monitor upload task timed out before completion',
                    completed_at = NOW()
                WHERE platform = ?
                  AND status IN ('accepted', 'running')
                  AND started_at IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, started_at, NOW()) > 7200
                """,
                PLATFORM
        );
    }

    public MonitorUploadTaskResponse status(String uploadTaskId) {
        ensureSchema();
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT * FROM " + TABLE + " WHERE upload_task_id = ?",
                    (rs, rowNum) -> {
                        Map<String, Object> result = parseJsonMap(rs.getString("result_json"));
                        BilibiliUploadResult upload = parseUploadResult(result);
                        String status = rs.getString("status");
                        boolean success = "success".equals(status);
                        String message = firstText(rs.getString("error_message"), upload == null ? "" : upload.message(), status);
                        String videoUrl = rs.getString("video_url");
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
                                message,
                                durationMs(result, startedAt, completedAt),
                                videoUrl,
                                upload == null ? null : upload.bvid(),
                                upload == null ? null : upload.aid(),
                                upload == null ? null : upload.accountUid(),
                                upload == null ? null : upload.accountName(),
                                upload == null || upload.raw() == null ? result : upload.raw(),
                                startedAt,
                                completedAt
                        );
                    },
                    uploadTaskId
            );
        } catch (EmptyResultDataAccessException exception) {
            return new MonitorUploadTaskResponse(false, uploadTaskId, PLATFORM, null, null, "missing", false,
                    "上传任务不存在", "TASK_NOT_FOUND", "上传任务不存在", null, null, null, null, null, null, Map.of(), null, null);
        }
    }

    private void execute(String uploadTaskId, BilibiliUploadRequest request) {
        long startedAt = System.currentTimeMillis();
        jdbcTemplate.update(
                "UPDATE " + TABLE + " SET status = 'running', started_at = COALESCE(started_at, NOW()), error_message = NULL WHERE upload_task_id = ?",
                uploadTaskId
        );
        try {
            BilibiliUploadResult result = uploadSynchronously(request);
            long durationMs = System.currentTimeMillis() - startedAt;
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("durationMs", durationMs);
            payload.put("result", result);
            jdbcTemplate.update(
                    "UPDATE " + TABLE + " SET status = 'success', result_json = ?, error_code = NULL, error_message = NULL, completed_at = NOW() WHERE upload_task_id = ?",
                    toJson(payload),
                    uploadTaskId
            );
        } catch (Exception exception) {
            long durationMs = System.currentTimeMillis() - startedAt;
            String message = firstText(exception.getMessage(), exception.getClass().getSimpleName());
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("durationMs", durationMs);
            payload.put("error", message);
            jdbcTemplate.update(
                    "UPDATE " + TABLE + " SET status = 'failed', result_json = ?, error_code = ?, error_message = ?, completed_at = NOW() WHERE upload_task_id = ?",
                    toJson(payload),
                    classifyErrorCode(message),
                    message,
                    uploadTaskId
            );
            log.warn("Bilibili async upload failed uploadTaskId={} taskId={} accountKey={} message={}",
                    uploadTaskId, request.taskId(), request.accountKey(), message, exception);
        }
    }

    private BilibiliUploadResult uploadSynchronously(BilibiliUploadRequest request) throws Exception {
        try {
            BilibiliUploadResult result = uploadService.upload(request);
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
            BilibiliUploadResult fallback = playwrightUploadService.upload(request);
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

    private String classifyErrorCode(String message) {
        String text = message == null ? "" : message.toLowerCase();
        if (text.contains("not logged in") || text.contains("login") || text.contains("登录")) {
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

    private BilibiliUploadResult parseUploadResult(Map<String, Object> result) {
        Object nested = result.get("result");
        if (nested == null) {
            return null;
        }
        return objectMapper.convertValue(nested, BilibiliUploadResult.class);
    }

    private Long durationMs(Map<String, Object> result, LocalDateTime startedAt, LocalDateTime completedAt) {
        Object value = result.get("durationMs");
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (startedAt != null && completedAt != null) {
            return java.time.Duration.between(startedAt, completedAt).toMillis();
        }
        return null;
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

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            return "{}";
        }
    }

    private String requestVideoUrl(BilibiliUploadRequest request) {
        return firstText(request.videoUrl(), request.minioUrl(), request.videoPath());
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

    private String text(String value) {
        return value == null ? "" : value.trim();
    }
}
