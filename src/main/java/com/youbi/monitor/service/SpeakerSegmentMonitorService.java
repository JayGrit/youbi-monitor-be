package com.youbi.monitor.service;

import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class SpeakerSegmentMonitorService {
    private static final int DEFAULT_LIMIT = 80;
    private static final int MAX_LIMIT = 200;

    private final MonitorRepository repository;

    public SpeakerSegmentMonitorService(MonitorRepository repository) {
        this.repository = repository;
    }

    public Map<String, Object> listSegments(MultiValueMap<String, String> query) {
        int page = positiveInt(first(query, "page"), 1);
        int limit = Math.min(MAX_LIMIT, positiveInt(first(query, "limit"), DEFAULT_LIMIT));
        QueryParts parts = where(query, true);
        long total = count(parts);
        int pageCount = total == 0 ? 0 : (int) Math.ceil((double) total / limit);
        int normalizedPage = pageCount == 0 ? 1 : Math.min(page, pageCount);
        int offset = (normalizedPage - 1) * limit;
        List<Map<String, Object>> items = list(parts, offset, limit).stream().map(this::normalizeSegment).toList();
        Map<String, Object> summary = summary(where(query, false));

        return Map.of(
                "items", items,
                "summary", summary,
                "total", total,
                "page", normalizedPage,
                "limit", limit,
                "pageCount", pageCount
        );
    }

    private long count(QueryParts parts) {
        Long count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM speaker_segment s
                LEFT JOIN task t ON t.id = s.task_id
                LEFT JOIN distributor_task_stages stage
                    ON stage.task_id = s.task_id AND stage.stage_name = 'speaker' AND stage.sub_stage = 'main'
                %s
                """.formatted(parts.where()), Long.class, parts.args().toArray());
        return count == null ? 0 : count;
    }

    private List<Map<String, Object>> list(QueryParts parts, int offset, int limit) {
        List<Object> args = new ArrayList<>(parts.args());
        args.add(Math.max(1, limit));
        args.add(Math.max(0, offset));
        return repository.queryForList("""
                SELECT
                    s.id,
                    s.task_id AS taskId,
                    COALESCE(NULLIF(t.task_type, ''), NULLIF(t.topic, ''), stage.stage_name) AS taskType,
                    COALESCE(t.priority, 1) AS priority,
                    s.item_index AS itemIndex,
                    s.status,
                    s.operator,
                    s.attempt_count AS attemptCount,
                    s.max_attempts AS maxAttempts,
                    s.started_at AS startedAt,
                    s.completed_at AS completedAt,
                    s.created_at AS createdAt,
                    CASE
                        WHEN s.status = 'running' AND s.started_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, s.started_at, NOW())
                        WHEN s.status IN ('success', 'failed') AND s.started_at IS NOT NULL AND s.completed_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, s.started_at, s.completed_at)
                        ELSE NULL
                    END AS elapsedSeconds,
                    CASE
                        WHEN s.started_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, s.created_at, s.started_at)
                        ELSE NULL
                    END AS waitingSeconds,
                    s.src_text AS srcText,
                    s.dst_text AS dstText,
                    s.speaker,
                    s.error_message AS errorMessage,
                    s.tts_wav_url AS ttsWavUrl,
                    s.reference_wav_url AS referenceWavUrl
                FROM speaker_segment s
                LEFT JOIN task t ON t.id = s.task_id
                LEFT JOIN distributor_task_stages stage
                    ON stage.task_id = s.task_id AND stage.stage_name = 'speaker' AND stage.sub_stage = 'main'
                %s
                ORDER BY
                    CASE
                        WHEN s.status IN ('success', 'failed') THEN 1
                        WHEN s.status = 'running' THEN 2
                        WHEN s.status IN ('ready', 'pending') THEN 3
                        ELSE 5
                    END ASC,
                    CASE WHEN s.status IN ('success', 'failed') THEN s.completed_at END ASC,
                    CASE WHEN s.status IN ('success', 'failed') THEN s.id END ASC,
                    CASE WHEN s.status = 'running' THEN s.started_at END ASC,
                    CASE WHEN s.status IN ('running', 'ready', 'pending') THEN COALESCE(t.priority, 1) END DESC,
                    CASE WHEN s.status IN ('ready', 'pending') THEN s.created_at END ASC,
                    CASE WHEN s.status IN ('ready', 'pending') THEN s.task_id END ASC,
                    CASE WHEN s.status IN ('ready', 'pending') THEN s.item_index END ASC,
                    s.id ASC
                LIMIT ? OFFSET ?
                """.formatted(parts.where()), args.toArray());
    }

    private Map<String, Object> summary(QueryParts parts) {
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT
                    COALESCE(NULLIF(TRIM(s.operator), ''), '未分配') AS device,
                    SUM(CASE WHEN s.status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN s.status = 'ready' THEN 1 ELSE 0 END) AS ready,
                    SUM(CASE WHEN s.status = 'running' THEN 1 ELSE 0 END) AS running,
                    SUM(CASE WHEN s.status = 'success' THEN 1 ELSE 0 END) AS success,
                    SUM(CASE WHEN s.status = 'failed' THEN 1 ELSE 0 END) AS failed,
                    COUNT(*) AS total,
                    MAX(CASE WHEN s.status = 'success' THEN s.completed_at ELSE NULL END) AS latestCompletedAt
                FROM speaker_segment s
                LEFT JOIN task t ON t.id = s.task_id
                LEFT JOIN distributor_task_stages stage
                    ON stage.task_id = s.task_id AND stage.stage_name = 'speaker' AND stage.sub_stage = 'main'
                %s
                GROUP BY COALESCE(NULLIF(TRIM(s.operator), ''), '未分配')
                ORDER BY device ASC
                """.formatted(parts.where()), parts.args().toArray());
        List<Map<String, Object>> devices = rows.stream().map(this::normalizeDeviceSummary).toList();
        long running = sum(devices, "running");
        long unfinished = devices.stream()
                .mapToLong(row -> number(row.get("pending")) + number(row.get("ready")) + number(row.get("running")))
                .sum();
        long completed = sum(devices, "success") + sum(devices, "failed");
        long failed = sum(devices, "failed");
        return Map.of(
                "devices", devices,
                "runningCount", running,
                "unfinishedCount", unfinished,
                "completedCount", completed,
                "failedCount", failed,
                "total", sum(devices, "total")
        );
    }

    private QueryParts where(MultiValueMap<String, String> query, boolean includeStatus) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        if (includeStatus) {
            addStatusFilter(conditions, args, query);
        } else {
            addDefaultScopeFilter(conditions, args, query);
        }
        addDeviceFilter(conditions, args, first(query, "device"));
        addLike(conditions, args, "s.task_id", first(query, "taskId"));
        addLike(conditions, args, "COALESCE(t.task_type, t.topic, '')", first(query, "taskType"));
        conditions.add("(t.status IS NULL OR t.status <> 'success')");
        return new QueryParts(conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions), args);
    }

    private void addStatusFilter(List<String> conditions, List<Object> args, MultiValueMap<String, String> query) {
        String status = text(first(query, "status"));
        if (status.isBlank() || "default".equals(status)) {
            addDefaultScopeFilter(conditions, args, query);
            return;
        }
        if ("unfinished".equals(status)) {
            conditions.add("s.status IN ('pending', 'ready', 'running')");
            return;
        }
        conditions.add("s.status = ?");
        args.add(status);
    }

    private void addDefaultScopeFilter(List<String> conditions, List<Object> args, MultiValueMap<String, String> query) {
        QueryParts completedFilters = recentCompletedFilters(query);
        conditions.add("""
                (s.status IN ('pending', 'ready', 'running') OR s.id IN (
                    SELECT recent.id
                    FROM (
                        SELECT s2.id
                        FROM speaker_segment s2
                        LEFT JOIN task t ON t.id = s2.task_id
                        %s
                        ORDER BY s2.completed_at DESC, s2.id DESC
                        LIMIT 20
                    ) recent
                ))
                """.formatted(completedFilters.where()));
        args.addAll(completedFilters.args());
    }

    private QueryParts recentCompletedFilters(MultiValueMap<String, String> query) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        conditions.add("s2.status IN ('success', 'failed')");
        addDeviceFilter(conditions, args, "s2.operator", first(query, "device"));
        addLike(conditions, args, "s2.task_id", first(query, "taskId"));
        addLike(conditions, args, "COALESCE(t.task_type, t.topic, '')", first(query, "taskType"));
        return new QueryParts(" WHERE " + String.join(" AND ", conditions), args);
    }

    private void addDeviceFilter(List<String> conditions, List<Object> args, String value) {
        addDeviceFilter(conditions, args, "s.operator", value);
    }

    private void addDeviceFilter(List<String> conditions, List<Object> args, String expression, String value) {
        String device = text(value);
        if (device.isBlank()) {
            return;
        }
        if ("__unassigned".equals(device) || "未分配".equals(device)) {
            conditions.add("(" + expression + " IS NULL OR TRIM(" + expression + ") = '')");
            return;
        }
        conditions.add(expression + " = ?");
        args.add(device);
    }

    private void addLike(List<String> conditions, List<Object> args, String expression, String value) {
        String text = text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(expression + " LIKE ?");
        args.add("%" + text + "%");
    }

    private Map<String, Object> normalizeSegment(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        put(result, row, "id");
        putText(result, row, "taskId");
        putText(result, row, "taskType");
        putNumber(result, row, "priority");
        putNumber(result, row, "itemIndex");
        putText(result, row, "status");
        String operator = text(row.get("operator"));
        result.put("operator", operator);
        result.put("device", operator.isBlank() ? "未分配" : operator);
        putNumber(result, row, "attemptCount");
        putNumber(result, row, "maxAttempts");
        put(result, row, "startedAt");
        put(result, row, "completedAt");
        put(result, row, "createdAt");
        putNullableNumber(result, row, "elapsedSeconds");
        putNullableNumber(result, row, "waitingSeconds");
        putText(result, row, "srcText");
        putText(result, row, "dstText");
        putText(result, row, "speaker");
        putText(result, row, "errorMessage");
        putText(result, row, "ttsWavUrl");
        putText(result, row, "referenceWavUrl");
        return result;
    }

    private Map<String, Object> normalizeDeviceSummary(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        putText(result, row, "device");
        putNumber(result, row, "pending");
        putNumber(result, row, "ready");
        putNumber(result, row, "running");
        putNumber(result, row, "success");
        putNumber(result, row, "failed");
        putNumber(result, row, "total");
        put(result, row, "latestCompletedAt");
        return result;
    }

    private String first(MultiValueMap<String, String> query, String key) {
        return query == null ? null : query.getFirst(key);
    }

    private int positiveInt(String value, int fallback) {
        try {
            int parsed = Integer.parseInt(text(value));
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

    private void putNullableNumber(Map<String, Object> result, Map<String, Object> row, String key) {
        Object value = row.get(key);
        result.put(key, value == null ? null : number(value));
    }

    private long sum(List<Map<String, Object>> rows, String key) {
        return rows.stream().mapToLong(row -> number(row.get(key))).sum();
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
}
