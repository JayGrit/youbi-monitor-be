package com.youbi.monitor.service;

import com.youbi.monitor.dto.BilibiliUploadRequest;
import com.youbi.monitor.dto.BilibiliUploadResult;
import com.youbi.monitor.dto.DouyinUploadRequest;
import com.youbi.monitor.dto.JinritoutiaoUploadRequest;
import com.youbi.monitor.dto.KuaishouUploadRequest;
import com.youbi.monitor.dto.MonitorUploadTaskResponse;
import com.youbi.monitor.dto.ShipinhaoUploadRequest;
import com.youbi.monitor.dto.XiaohongshuUploadRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youbi.monitor.model.MonitorUploadTaskRow;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.youbi.monitor.repository.IMonitorAsyncUploadRepositoryService;
import org.springframework.beans.factory.annotation.Value;
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
    private static final int MAX_BROWSER_SESSION_UPLOAD_ATTEMPTS = 3;
    private static final long BROWSER_SESSION_RETRY_WINDOW_MS = 30_000L;
    private static final long BROWSER_SESSION_RETRY_DELAY_MS = 3_000L;
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final BilibiliUploadService bilibiliUploadService;
    private final BilibiliPlaywrightUploadService bilibiliPlaywrightUploadService;
    private final XiaohongshuUploadService xiaohongshuUploadService;
    private final DouyinUploadService douyinUploadService;
    private final ShipinhaoUploadService shipinhaoUploadService;
    private final KuaishouUploadService kuaishouUploadService;
    private final JinritoutiaoUploadService jinritoutiaoUploadService;
    private final IMonitorAsyncUploadRepositoryService repositoryService;
    private final ObjectMapper objectMapper;
    private final int defaultUploadTaskTimeoutSeconds;
    private final int alidriveUploadTaskTimeoutSeconds;
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
            JinritoutiaoUploadService jinritoutiaoUploadService,
            IMonitorAsyncUploadRepositoryService repositoryService,
            ObjectMapper objectMapper,
            @Value("${youbi.upload-task-timeout-seconds:480}") int defaultUploadTaskTimeoutSeconds,
            @Value("${youbi.alidrive-upload-task-timeout-seconds:900}") int alidriveUploadTaskTimeoutSeconds
    ) {
        this.bilibiliUploadService = bilibiliUploadService;
        this.bilibiliPlaywrightUploadService = bilibiliPlaywrightUploadService;
        this.xiaohongshuUploadService = xiaohongshuUploadService;
        this.douyinUploadService = douyinUploadService;
        this.shipinhaoUploadService = shipinhaoUploadService;
        this.kuaishouUploadService = kuaishouUploadService;
        this.jinritoutiaoUploadService = jinritoutiaoUploadService;
        this.repositoryService = repositoryService;
        this.objectMapper = objectMapper;
        this.defaultUploadTaskTimeoutSeconds = Math.max(60, defaultUploadTaskTimeoutSeconds);
        this.alidriveUploadTaskTimeoutSeconds = Math.max(this.defaultUploadTaskTimeoutSeconds, alidriveUploadTaskTimeoutSeconds);
    }

    @PostConstruct
    public void ensureSchema() {
        repositoryService.ensureSchema();
    }

    public synchronized MonitorUploadTaskResponse submit(String platform, Object request) {
        ensureSchema();
        failStaleRunningTasks();
        long running = repositoryService.countActiveTasks();
        String normalizedPlatform = text(platform).toLowerCase();
        if (running > 0) {
            return new MonitorUploadTaskResponse(false, null, normalizedPlatform, taskId(request), accountKey(request),
                    "rejected", false, "已有上传任务正在执行", "UPLOAD_RUNNING", "已有上传任务正在执行",
                    null, null, null, null, null, Map.of(), null, null);
        }

        String uploadTaskId = UUID.randomUUID().toString();
        repositoryService.insertAcceptedTask(
                uploadTaskId,
                normalizedPlatform,
                taskId(request),
                defaultText(accountKey(request), "default"),
                uploadTimeoutSeconds(request)
        );
        executor.submit(() -> execute(uploadTaskId, normalizedPlatform, request));
        return status(uploadTaskId);
    }

    public MonitorUploadTaskResponse status(String uploadTaskId) {
        ensureSchema();
        failStaleRunningTasks();
        return repositoryService.findByUploadTaskId(uploadTaskId)
                .map(this::response)
                .orElseGet(() -> new MonitorUploadTaskResponse(false, uploadTaskId, null, null, null, "missing", false,
                        "上传任务不存在", "TASK_NOT_FOUND", "上传任务不存在", null, null, null, null, null, Map.of(), null, null));
    }

    private void execute(String uploadTaskId, String platform, Object request) {
        long startedAt = System.currentTimeMillis();
        if (!repositoryService.markRunning(uploadTaskId)) {
            log.warn("Async upload skipped because task is no longer accepted uploadTaskId={} platform={} taskId={}",
                    uploadTaskId, platform, taskId(request));
            return;
        }
        try {
            UploadAttemptResult attemptResult = uploadWithBrowserSessionRetry(uploadTaskId, platform, request, startedAt);
            Object result = attemptResult.result();
            Map<String, Object> resultMap = objectMapper.convertValue(result, MAP_TYPE);
            if (!Boolean.TRUE.equals(resultMap.get("success"))) {
                throw new IllegalStateException(stringValue(resultMap.get("message")));
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("durationMs", System.currentTimeMillis() - startedAt);
            payload.put("attempts", attemptResult.attempts());
            payload.put("result", resultMap);
            if (!repositoryService.markSuccess(uploadTaskId, toJson(payload))) {
                log.warn("Async upload finished after task status changed uploadTaskId={} platform={} taskId={}",
                        uploadTaskId, platform, taskId(request));
            }
        } catch (Exception exception) {
            String message = firstText(exception.getMessage(), exception.getClass().getSimpleName());
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("durationMs", System.currentTimeMillis() - startedAt);
            payload.put("error", message);
            boolean updated = repositoryService.markFailed(
                    uploadTaskId,
                    toJson(payload),
                    classifyErrorCode(exception, message),
                    message
            );
            if (!updated) {
                log.warn("Async upload failed after task status changed uploadTaskId={} platform={} taskId={} message={}",
                        uploadTaskId, platform, taskId(request), message);
                return;
            }
            log.warn("Async upload failed uploadTaskId={} platform={} taskId={} accountKey={} message={}",
                    uploadTaskId, platform, taskId(request), accountKey(request), message, exception);
        }
    }

    private UploadAttemptResult uploadWithBrowserSessionRetry(String uploadTaskId, String platform, Object request, long startedAt) throws Exception {
        int attempt = 1;
        while (true) {
            try {
                return new UploadAttemptResult(upload(platform, request), attempt);
            } catch (Exception exception) {
                if (!shouldRetryBrowserSessionLoss(exception, startedAt, attempt)) {
                    throw exception;
                }
                log.warn(
                        "Async upload browser session lost, retrying uploadTaskId={} platform={} taskId={} accountKey={} attempt={} nextAttempt={} message={}",
                        uploadTaskId,
                        platform,
                        taskId(request),
                        accountKey(request),
                        attempt,
                        attempt + 1,
                        firstText(exception.getMessage(), exception.getClass().getSimpleName())
                );
                sleepBeforeBrowserSessionRetry();
                attempt += 1;
            }
        }
    }

    private boolean shouldRetryBrowserSessionLoss(Exception exception, long startedAt, int attempt) {
        if (attempt >= MAX_BROWSER_SESSION_UPLOAD_ATTEMPTS) {
            return false;
        }
        if (System.currentTimeMillis() - startedAt > BROWSER_SESSION_RETRY_WINDOW_MS) {
            return false;
        }
        return isBrowserSessionLost(exception);
    }

    private void sleepBeforeBrowserSessionRetry() {
        try {
            Thread.sleep(BROWSER_SESSION_RETRY_DELAY_MS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted before retrying browser session upload", interrupted);
        }
    }

    private Object upload(String platform, Object request) throws Exception {
        return switch (platform) {
            case "bilibili" -> uploadBilibili((BilibiliUploadRequest) request);
            case "xiaohongshu" -> xiaohongshuUploadService.upload((XiaohongshuUploadRequest) request);
            case "douyin" -> douyinUploadService.upload((DouyinUploadRequest) request);
            case "shipinhao" -> shipinhaoUploadService.upload((ShipinhaoUploadRequest) request);
            case "kuaishou" -> kuaishouUploadService.upload((KuaishouUploadRequest) request);
            case "jinritoutiao" -> jinritoutiaoUploadService.upload((JinritoutiaoUploadRequest) request);
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
        repositoryService.failStaleRunningTasks();
    }

    private int uploadTimeoutSeconds(Object request) {
        return usesAliDrive(request) ? alidriveUploadTaskTimeoutSeconds : defaultUploadTaskTimeoutSeconds;
    }

    private boolean usesAliDrive(Object request) {
        String videoLocation = "";
        String alidriveFileId = "";
        String alidriveRemotePath = "";
        if (request instanceof BilibiliUploadRequest value) {
            videoLocation = value.videoLocation();
            alidriveFileId = value.alidriveFileId();
            alidriveRemotePath = value.alidriveRemotePath();
        } else if (request instanceof XiaohongshuUploadRequest value) {
            videoLocation = value.videoLocation();
            alidriveFileId = value.alidriveFileId();
            alidriveRemotePath = value.alidriveRemotePath();
        } else if (request instanceof DouyinUploadRequest value) {
            videoLocation = value.videoLocation();
            alidriveFileId = value.alidriveFileId();
            alidriveRemotePath = value.alidriveRemotePath();
        } else if (request instanceof ShipinhaoUploadRequest value) {
            videoLocation = value.videoLocation();
            alidriveFileId = value.alidriveFileId();
            alidriveRemotePath = value.alidriveRemotePath();
        } else if (request instanceof KuaishouUploadRequest value) {
            videoLocation = value.videoLocation();
            alidriveFileId = value.alidriveFileId();
            alidriveRemotePath = value.alidriveRemotePath();
        } else if (request instanceof JinritoutiaoUploadRequest value) {
            videoLocation = value.videoLocation();
            alidriveFileId = value.alidriveFileId();
            alidriveRemotePath = value.alidriveRemotePath();
        }
        String location = text(videoLocation).toLowerCase();
        return location.equals("adrive")
                || location.equals("alidrive")
                || location.equals("aliyun")
                || location.equals("aliyundrive")
                || !text(alidriveFileId).isBlank()
                || !text(alidriveRemotePath).isBlank();
    }

    private MonitorUploadTaskResponse response(MonitorUploadTaskRow row) {
        Map<String, Object> result = parseJsonMap(row.resultJson());
        Map<String, Object> upload = uploadResult(result);
        String status = row.status();
        boolean success = "success".equals(status);
        return new MonitorUploadTaskResponse(
                true,
                row.uploadTaskId(),
                row.platform(),
                row.upstreamTaskId(),
                row.accountKey(),
                status,
                success,
                null,
                row.errorCode(),
                firstText(row.errorMessage(), stringValue(upload.get("message")), status),
                durationMs(result, row.startedAt(), row.completedAt()),
                stringValue(upload.get("bvid")),
                longValue(upload.get("aid")),
                longValue(upload.get("accountUid")),
                firstText(stringValue(upload.get("accountName")), stringValue(upload.get("accountKey"))),
                rawResult(upload, result),
                row.startedAt(),
                row.completedAt()
        );
    }

    private String classifyErrorCode(Exception exception, String message) {
        PlaywrightUploadException playwrightException = playwrightUploadException(exception);
        if (playwrightException != null) {
            return playwrightException.errorCode().code();
        }
        if (isBrowserSessionLost(exception) || isBrowserSessionLost(message)) {
            return PlaywrightUploadErrorCode.BROWSER_SESSION_LOST.code();
        }
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

    private PlaywrightUploadException playwrightUploadException(Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof PlaywrightUploadException playwrightException) {
                return playwrightException;
            }
            current = current.getCause();
        }
        return null;
    }

    private boolean isBrowserSessionLost(Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            if (isBrowserSessionLost(current.getMessage())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isBrowserSessionLost(String message) {
        String text = message == null ? "" : message.toLowerCase();
        return text.contains("object doesn't exist:")
                || text.contains("target page, context or browser has been closed")
                || text.contains("browser has been closed")
                || text.contains("browser closed")
                || text.contains("connection closed")
                || text.contains("playwright connection closed")
                || text.contains("execution context was destroyed");
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

    private record UploadAttemptResult(Object result, int attempts) {
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
        if (request instanceof JinritoutiaoUploadRequest item) {
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
        if (request instanceof JinritoutiaoUploadRequest item) {
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
