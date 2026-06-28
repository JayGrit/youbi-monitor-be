package com.youbi.monitor.service;

import com.youbi.monitor.repository.QueueMonitorRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class QueueMonitorService {
    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;
    private static final DateTimeFormatter SQL_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final QueueMonitorRepository repository;

    public QueueMonitorService(QueueMonitorRepository repository) {
        this.repository = repository;
    }

    public Map<String, Object> listFfmpegerQueue(MultiValueMap<String, String> query) {
        return pagedQueue(query, this::ffmpegerWhere, this::countFfmpegerQueue, this::listFfmpegerQueue, this::normalizeFfmpegerTask);
    }

    public Map<String, Object> listAirouterQueue(MultiValueMap<String, String> query) {
        return pagedQueue(query, this::airouterWhere, this::countAirouterQueue, this::listAirouterQueue, this::normalizeAirouterTask);
    }

    private Map<String, Object> pagedQueue(
            MultiValueMap<String, String> query,
            FilterBuilder filterBuilder,
            Counter counter,
            Lister lister,
            Normalizer normalizer
    ) {
        int page = positiveInt(first(query, "page"), 1);
        int limit = Math.min(MAX_LIMIT, positiveInt(first(query, "limit"), DEFAULT_LIMIT));
        Map<String, String> filters = filters(query);
        QueryParts parts = filterBuilder.build(filters);
        long total = counter.count(parts);
        int pageCount = total == 0 ? 0 : (int) Math.ceil((double) total / limit);
        int normalizedPage = pageCount == 0 ? 1 : Math.min(page, pageCount);
        int offset = (normalizedPage - 1) * limit;
        List<Map<String, Object>> items = lister.list(parts, offset, limit)
                .stream()
                .map(normalizer::normalize)
                .toList();
        return Map.of(
                "items", items,
                "total", total,
                "page", normalizedPage,
                "limit", limit,
                "pageCount", pageCount
        );
    }

    private long countFfmpegerQueue(QueryParts parts) {
        Long count = repository.queryForObject(
                "SELECT COUNT(*) FROM ffmpeger_task t LEFT JOIN ffmpeger_task_type type ON type.task_type = t.task_type " + parts.where(),
                Long.class,
                parts.args().toArray()
        );
        return count == null ? 0 : count;
    }

    private List<Map<String, Object>> listFfmpegerQueue(QueryParts parts, int offset, int limit) {
        List<Object> args = new ArrayList<>(parts.args());
        args.add(Math.max(1, limit));
        args.add(Math.max(0, offset));
        return repository.queryForList("""
                SELECT
                    t.id,
                    t.op_id AS opId,
                    t.run_id AS runId,
                    t.task_id AS taskId,
                    t.task_type AS taskType,
                    COALESCE(NULLIF(type.note, ''), type.display_name) AS taskTypeDisplayName,
                    t.run_number AS runNumber,
                    t.status,
                    t.priority,
                    t.attempt_count AS attemptCount,
                    t.worker_id AS workerId,
                    t.available_at AS availableAt,
                    t.heartbeat_at AS heartbeatAt,
                    t.lease_expires_at AS leaseExpiresAt,
                    t.timeout_seconds AS timeoutSeconds,
                    t.created_at AS createdAt,
                    t.started_at AS startedAt,
                    t.completed_at AS completedAt,
                    t.updated_at AS updatedAt,
                    t.error_code AS errorCode,
                    t.error_message AS errorMessage
                FROM ffmpeger_task t
                LEFT JOIN ffmpeger_task_type type ON type.task_type = t.task_type
                %s
                ORDER BY
                    CASE
                        WHEN t.status = 'running' THEN 3
                        WHEN t.status = 'ready' THEN 2
                        WHEN t.status = 'failed' THEN 1
                        ELSE 0
                    END DESC,
                    CASE WHEN t.status = 'ready' THEN t.priority ELSE 0 END DESC,
                    t.available_at ASC,
                    t.created_at DESC,
                    t.id DESC
                LIMIT ? OFFSET ?
                """.formatted(parts.where()), args.toArray());
    }

    private long countAirouterQueue(QueryParts parts) {
        Long count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM airouter_task t
                LEFT JOIN airouter_task_type type ON type.task_type = t.task_type
                LEFT JOIN airouter_task_detail detail ON detail.task_id = t.id
                %s
                """.formatted(parts.where()),
                Long.class,
                parts.args().toArray()
        );
        return count == null ? 0 : count;
    }

    private List<Map<String, Object>> listAirouterQueue(QueryParts parts, int offset, int limit) {
        List<Object> args = new ArrayList<>(parts.args());
        args.add(Math.max(1, limit));
        args.add(Math.max(0, offset));
        return repository.queryForList("""
                SELECT
                    t.id,
                    t.task_type AS taskType,
                    COALESCE(NULLIF(type.note, ''), type.name) AS taskTypeDisplayName,
                    t.caller,
                    t.upstream_task_id AS upstreamTaskId,
                    t.request_key AS requestKey,
                    t.status,
                    t.priority,
                    t.attempt_count AS attemptCount,
                    t.max_attempts AS maxAttempts,
                    t.next_run_at AS nextRunAt,
                    t.operator,
                    detail.model,
                    detail.api_key_id AS apiKeyId,
                    detail.input_chars AS inputChars,
                    detail.output_chars AS outputChars,
                    t.created_at AS createdAt,
                    t.started_at AS startedAt,
                    t.completed_at AS completedAt,
                    t.updated_at AS updatedAt,
                    detail.error_code AS errorCode,
                    detail.error_message AS errorMessage
                FROM airouter_task t
                LEFT JOIN airouter_task_type type ON type.task_type = t.task_type
                LEFT JOIN airouter_task_detail detail ON detail.task_id = t.id
                %s
                ORDER BY
                    CASE
                        WHEN t.status = 'running' THEN 3
                        WHEN t.status = 'pending' THEN 2
                        WHEN t.status = 'failed' THEN 1
                        ELSE 0
                    END DESC,
                    CASE WHEN t.status = 'pending' THEN t.priority ELSE 0 END DESC,
                    t.next_run_at ASC,
                    t.created_at DESC,
                    t.id DESC
                LIMIT ? OFFSET ?
                """.formatted(parts.where()), args.toArray());
    }

    private QueryParts ffmpegerWhere(Map<String, String> filters) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        addEqual(conditions, args, "t.status", filters.get("status"));
        addPrefix(conditions, args, "t.task_type", filters.get("taskTypePrefix"));
        addLike(conditions, args, "t.task_type", filters.get("taskType"));
        addLike(conditions, args, "t.task_id", filters.get("taskId"));
        addLike(conditions, args, "t.op_id", filters.get("opId"));
        addLike(conditions, args, "t.worker_id", filters.get("workerId"));
        addDateLowerBound(conditions, args, "t.created_at", filters.get("createdFrom"));
        addDateUpperBound(conditions, args, "t.created_at", filters.get("createdTo"));
        return where(conditions, args);
    }

    private QueryParts airouterWhere(Map<String, String> filters) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        addEqual(conditions, args, "t.status", filters.get("status"));
        addLike(conditions, args, "t.caller", filters.get("caller"));
        addLike(conditions, args, "t.task_type", filters.get("taskType"));
        addLike(conditions, args, "t.upstream_task_id", filters.get("upstreamTaskId"));
        addLike(conditions, args, "t.request_key", filters.get("requestKey"));
        addLike(conditions, args, "t.operator", filters.get("operator"));
        addDateLowerBound(conditions, args, "t.created_at", filters.get("createdFrom"));
        addDateUpperBound(conditions, args, "t.created_at", filters.get("createdTo"));
        return where(conditions, args);
    }

    private QueryParts where(List<String> conditions, List<Object> args) {
        String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
        return new QueryParts(where, args);
    }

    private Map<String, Object> normalizeFfmpegerTask(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        put(result, row, "id");
        putText(result, row, "opId");
        putText(result, row, "runId");
        putText(result, row, "taskId");
        putText(result, row, "taskType");
        putText(result, row, "taskTypeDisplayName");
        putNumber(result, row, "runNumber");
        putText(result, row, "status");
        putNumber(result, row, "priority");
        putNumber(result, row, "attemptCount");
        putText(result, row, "workerId");
        put(result, row, "availableAt");
        put(result, row, "heartbeatAt");
        put(result, row, "leaseExpiresAt");
        putNumber(result, row, "timeoutSeconds");
        put(result, row, "createdAt");
        put(result, row, "startedAt");
        put(result, row, "completedAt");
        put(result, row, "updatedAt");
        putText(result, row, "errorCode");
        putText(result, row, "errorMessage");
        return result;
    }

    private Map<String, Object> normalizeAirouterTask(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        put(result, row, "id");
        putText(result, row, "taskType");
        putText(result, row, "taskTypeDisplayName");
        putText(result, row, "caller");
        putText(result, row, "upstreamTaskId");
        putText(result, row, "requestKey");
        putText(result, row, "status");
        putNumber(result, row, "priority");
        putNumber(result, row, "attemptCount");
        putNumber(result, row, "maxAttempts");
        put(result, row, "nextRunAt");
        putText(result, row, "operator");
        putText(result, row, "model");
        putNumber(result, row, "apiKeyId");
        putNumber(result, row, "inputChars");
        putNumber(result, row, "outputChars");
        put(result, row, "createdAt");
        put(result, row, "startedAt");
        put(result, row, "completedAt");
        put(result, row, "updatedAt");
        putText(result, row, "errorCode");
        putText(result, row, "errorMessage");
        return result;
    }

    private Map<String, String> filters(MultiValueMap<String, String> query) {
        Map<String, String> filters = new LinkedHashMap<>();
        for (String key : List.of("status", "taskType", "taskTypePrefix", "taskId", "opId", "workerId", "caller", "upstreamTaskId", "requestKey", "operator")) {
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

    private void addEqual(List<String> conditions, List<Object> args, String column, String value) {
        String text = TextSupport.text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " = ?");
        args.add(text);
    }

    private void addLike(List<String> conditions, List<Object> args, String column, String value) {
        String text = TextSupport.text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " LIKE ?");
        args.add("%" + text + "%");
    }

    private void addPrefix(List<String> conditions, List<Object> args, String column, String value) {
        String text = TextSupport.text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " LIKE ?");
        args.add(text + "%");
    }

    private void addDateLowerBound(List<String> conditions, List<Object> args, String column, String value) {
        String text = TextSupport.text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " >= ?");
        args.add(text);
    }

    private void addDateUpperBound(List<String> conditions, List<Object> args, String column, String value) {
        String text = TextSupport.text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " < ?");
        args.add(text);
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

    private void put(Map<String, Object> result, Map<String, Object> row, String key) {
        result.put(key, row.get(key));
    }

    private void putText(Map<String, Object> result, Map<String, Object> row, String key) {
        result.put(key, text(row.get(key)));
    }

    private void putNumber(Map<String, Object> result, Map<String, Object> row, String key) {
        result.put(key, number(row.get(key)));
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

    private String text(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private record QueryParts(String where, List<Object> args) {
    }

    @FunctionalInterface
    private interface FilterBuilder {
        QueryParts build(Map<String, String> filters);
    }

    @FunctionalInterface
    private interface Counter {
        long count(QueryParts parts);
    }

    @FunctionalInterface
    private interface Lister {
        List<Map<String, Object>> list(QueryParts parts, int offset, int limit);
    }

    @FunctionalInterface
    private interface Normalizer {
        Map<String, Object> normalize(Map<String, Object> row);
    }
}
