package com.youbi.monitor.service;

import com.youbi.monitor.model.DiagnosticArtifactRecord;
import com.youbi.monitor.repository.IDiagnosticArtifactRepositoryService;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

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

    public OperatorDiagnosticsService(IDiagnosticArtifactRepositoryService repositoryService) {
        this.repositoryService = repositoryService;
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
        result.put("accountKey", text(row.get("accountKey")));
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
        result.put("htmlUrl", row.htmlUrl());
        result.put("html_url", row.htmlUrl());
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
        for (String key : List.of("status", "platform", "action", "taskId", "opId", "accountKey")) {
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
}
