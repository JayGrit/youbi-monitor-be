package com.youbi.monitor.service;

import com.youbi.monitor.model.DiagnosticArtifactRecord;
import com.youbi.monitor.repository.IDiagnosticArtifactRepositoryService;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.net.URI;
import java.net.URLDecoder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class OperatorDiagnosticsService {
    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 100;
    private static final DateTimeFormatter SQL_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final IDiagnosticArtifactRepositoryService repositoryService;
    private final MinioClient minioClient;
    private final String minioEndpointHost;

    public OperatorDiagnosticsService(
            IDiagnosticArtifactRepositoryService repositoryService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey
    ) {
        this.repositoryService = repositoryService;
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioEndpointHost = endpointHost(minioEndpoint);
    }

    public Map<String, Object> listTasks(MultiValueMap<String, String> query) {
        int page = positiveInt(first(query, "page"), 1);
        int limit = Math.min(MAX_LIMIT, positiveInt(first(query, "limit"), DEFAULT_LIMIT));
        Map<String, String> filters = filters(query);
        long total = repositoryService.countOperatorExecutions(filters);
        int pageCount = total == 0 ? 0 : (int) Math.ceil((double) total / limit);
        int normalizedPage = pageCount == 0 ? 1 : Math.min(page, pageCount);
        int offset = (normalizedPage - 1) * limit;
        List<Map<String, Object>> items = repositoryService.listOperatorExecutions(filters, offset, limit)
                .stream()
                .map(this::normalizeExecution)
                .toList();
        return Map.of(
                "items", items,
                "total", total,
                "page", normalizedPage,
                "limit", limit,
                "pageCount", pageCount
        );
    }

    public Map<String, Object> listQueue(MultiValueMap<String, String> query) {
        int page = positiveInt(first(query, "page"), 1);
        int limit = Math.min(MAX_LIMIT, positiveInt(first(query, "limit"), 50));
        Map<String, String> filters = filters(query);
        long total = repositoryService.countOperatorQueue(filters);
        int pageCount = total == 0 ? 0 : (int) Math.ceil((double) total / limit);
        int normalizedPage = pageCount == 0 ? 1 : Math.min(page, pageCount);
        int offset = (normalizedPage - 1) * limit;
        List<Map<String, Object>> items = repositoryService.listOperatorQueue(filters, offset, limit)
                .stream()
                .map(this::normalizeQueueTask)
                .toList();
        return Map.of(
                "items", items,
                "total", total,
                "page", normalizedPage,
                "limit", limit,
                "pageCount", pageCount
        );
    }

    public Map<String, Object> getTask(String opId) {
        Map<String, String> filters = Map.of("opId", TextSupport.text(opId));
        return repositoryService.listOperatorExecutions(filters, 0, 1)
                .stream()
                .findFirst()
                .map(this::normalizeExecution)
                .orElseGet(() -> Map.of("opId", TextSupport.text(opId), "status", "not_found", "diagnosticCount", 0));
    }

    public Map<String, Object> getDiagnostics(String opId, MultiValueMap<String, String> query) {
        int page = positiveInt(first(query, "page"), 1);
        int limit = Math.min(MAX_LIMIT, positiveInt(first(query, "limit"), DEFAULT_LIMIT));
        long total = repositoryService.countOperatorDiagnostics(TextSupport.text(opId));
        int pageCount = total == 0 ? 0 : (int) Math.ceil((double) total / limit);
        int normalizedPage = pageCount == 0 ? 1 : Math.min(page, pageCount);
        int offset = (normalizedPage - 1) * limit;
        List<Map<String, Object>> items = repositoryService.listOperatorDiagnostics(TextSupport.text(opId), offset, limit)
                .stream()
                .map(this::diagnosticJson)
                .toList();
        return Map.of(
                "items", items,
                "total", total,
                "page", normalizedPage,
                "limit", limit,
                "pageCount", pageCount
        );
    }

    public ResponseEntity<StreamingResponseBody> getDiagnosticArtifact(long id, String kind) {
        DiagnosticArtifactRecord row = repositoryService.getOperatorDiagnostic(id);
        if (row == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "diagnostic artifact not found");
        }
        String url = switch (text(kind)) {
            case "screenshot" -> row.screenshotUrl();
            case "html" -> row.htmlUrl();
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unsupported artifact kind");
        };
        MinioObjectRef objectRef = minioObjectRef(url);
        if (objectRef == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "diagnostic artifact url is empty or unsupported");
        }
        String filename = filename(objectRef.objectName());
        StreamingResponseBody body = output -> {
            try (GetObjectResponse input = minioClient.getObject(GetObjectArgs.builder()
                    .bucket(objectRef.bucket())
                    .object(objectRef.objectName())
                    .build())) {
                input.transferTo(output);
            } catch (Exception exception) {
                throw exception instanceof IOException ioException ? ioException : new IOException("Failed to read diagnostic artifact", exception);
            }
        };
        return ResponseEntity.ok()
                .contentType(mediaType(filename, kind))
                .header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.inline().filename(filename, StandardCharsets.UTF_8).build().toString())
                .body(body);
    }

    private Map<String, Object> normalizeExecution(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        String opId = text(row.get("opId"));
        String platform = text(row.get("platform"));
        String diagnosticPath = text(row.get("diagnosticPath"));
        String action = text(row.get("taskAction"));
        if (!diagnosticPath.isBlank()) {
            String[] parts = diagnosticPath.split("/", 2);
            if (action.isBlank() && parts.length > 1) {
                action = parts[1];
            }
        }
        result.put("opId", opId);
        result.put("taskId", text(row.get("taskId")));
        result.put("platform", platform);
        result.put("action", action);
        result.put("topic", text(row.get("topic")));
        result.put("status", text(row.get("status")));
        result.put("diagnosticCount", number(row.get("diagnosticCount")));
        result.put("createdAt", row.get("createdAt"));
        result.put("startedAt", row.get("startedAt"));
        result.put("completedAt", row.get("completedAt"));
        String errorMessage = text(row.get("errorMessage"));
        if (!errorMessage.isBlank()) {
            result.put("error", Map.of("message", errorMessage));
        }
        return result;
    }

    private Map<String, Object> normalizeQueueTask(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", row.get("id"));
        result.put("opId", text(row.get("opId")));
        result.put("runId", text(row.get("runId")));
        result.put("taskId", text(row.get("taskId")));
        result.put("platform", text(row.get("platform")));
        result.put("action", text(row.get("action")));
        result.put("taskType", text(row.get("taskType")));
        result.put("taskTypeDisplayName", text(row.get("taskTypeDisplayName")));
        result.put("topic", text(row.get("topic")));
        result.put("status", text(row.get("status")));
        result.put("priority", number(row.get("priority")));
        result.put("createdAt", row.get("createdAt"));
        result.put("startedAt", row.get("startedAt"));
        result.put("completedAt", row.get("completedAt"));
        String errorCode = text(row.get("errorCode"));
        String errorMessage = text(row.get("errorMessage"));
        if (!errorCode.isBlank()) {
            result.put("errorCode", errorCode);
        }
        if (!errorMessage.isBlank()) {
            result.put("errorMessage", errorMessage);
        }
        return result;
    }

    private Map<String, Object> diagnosticJson(DiagnosticArtifactRecord row) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", row.id());
        result.put("opId", row.runId());
        result.put("op_id", row.runId());
        result.put("taskId", row.taskId());
        result.put("runId", row.runId());
        result.put("stepIndex", row.stepIndex());
        result.put("step_index", row.stepIndex());
        result.put("stepName", row.stepName());
        result.put("step_name", row.stepName());
        result.put("screenshotUrl", row.screenshotUrl());
        result.put("screenshot_url", row.screenshotUrl());
        result.put("screenshotProxyPath", "operator/diagnostics/" + row.id() + "/screenshot");
        result.put("screenshot_proxy_path", "operator/diagnostics/" + row.id() + "/screenshot");
        result.put("htmlUrl", row.htmlUrl());
        result.put("html_url", row.htmlUrl());
        result.put("htmlProxyPath", "operator/diagnostics/" + row.id() + "/html");
        result.put("html_proxy_path", "operator/diagnostics/" + row.id() + "/html");
        result.put("screenshotSizeBytes", row.screenshotSizeBytes());
        result.put("htmlSizeBytes", row.htmlSizeBytes());
        result.put("screenshotWidth", row.screenshotWidth());
        result.put("screenshotHeight", row.screenshotHeight());
        result.put("status", row.status());
        result.put("errorMessage", row.errorMessage());
        result.put("createdAt", row.createdAt());
        return result;
    }

    private Map<String, String> filters(MultiValueMap<String, String> query) {
        Map<String, String> filters = new LinkedHashMap<>();
        for (String key : List.of("status", "platform", "action", "taskId", "opId", "topic")) {
            String value = first(query, key);
            if (value != null && !value.isBlank()) {
                filters.put(key, value.trim());
            }
        }
        putDateTimeFilter(filters, "createdFrom", first(query, "createdFrom"));
        putDateTimeFilter(filters, "createdTo", first(query, "createdTo"));
        return filters;
    }

    private void putDateTimeFilter(Map<String, String> filters, String key, String value) {
        LocalDateTime dateTime = parseDateTime(value);
        if (dateTime != null) {
            filters.put(key, SQL_DATETIME.format(dateTime));
        }
    }

    private LocalDateTime parseDateTime(String value) {
        String text = TextSupport.text(value);
        if (text.isBlank()) {
            return null;
        }
        try {
            return LocalDateTime.parse(text.replace(' ', 'T'));
        } catch (DateTimeParseException exception) {
            try {
                return LocalDateTime.parse(text, SQL_DATETIME);
            } catch (DateTimeParseException ignored) {
                return null;
            }
        }
    }

    private String first(MultiValueMap<String, String> query, String key) {
        return query == null ? null : query.getFirst(key);
    }

    private int positiveInt(String value, int fallback) {
        try {
            int parsed = Integer.parseInt(TextSupport.text(value));
            return parsed > 0 ? parsed : fallback;
        } catch (Exception exception) {
            return fallback;
        }
    }

    private String text(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private MinioObjectRef minioObjectRef(String url) {
        String text = text(url);
        if (text.isBlank()) {
            return null;
        }
        try {
            URI uri = URI.create(text);
            String host = endpointHost(uri);
            if (!minioEndpointHost.isBlank() && !minioEndpointHost.equals(host)) {
                return null;
            }
            String path = uri.getRawPath() == null ? "" : uri.getRawPath().replaceFirst("^/+", "");
            String[] parts = path.split("/", 2);
            if (parts.length < 2 || parts[0].isBlank() || parts[1].isBlank()) {
                return null;
            }
            return new MinioObjectRef(decode(parts[0]), decode(parts[1]));
        } catch (Exception exception) {
            return null;
        }
    }

    private String endpointHost(String endpoint) {
        try {
            return endpointHost(URI.create(endpoint));
        } catch (Exception exception) {
            return "";
        }
    }

    private String endpointHost(URI uri) {
        String host = text(uri.getHost()).toLowerCase();
        int port = uri.getPort();
        return port > 0 ? host + ":" + port : host;
    }

    private String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private String filename(String objectName) {
        String text = text(objectName);
        int slash = text.lastIndexOf('/');
        String filename = slash >= 0 ? text.substring(slash + 1) : text;
        return filename.isBlank() ? "diagnostic-artifact" : filename;
    }

    private MediaType mediaType(String filename, String kind) {
        String lower = text(filename).toLowerCase();
        if ("html".equals(text(kind)) || lower.endsWith(".html") || lower.endsWith(".htm")) {
            return MediaType.TEXT_HTML;
        }
        if (lower.endsWith(".png")) {
            return MediaType.IMAGE_PNG;
        }
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) {
            return MediaType.IMAGE_JPEG;
        }
        if (lower.endsWith(".webp")) {
            return MediaType.parseMediaType("image/webp");
        }
        return MediaType.APPLICATION_OCTET_STREAM;
    }

    private long number(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(text(value));
        } catch (Exception exception) {
            return 0;
        }
    }

    private record MinioObjectRef(String bucket, String objectName) {
    }
}
