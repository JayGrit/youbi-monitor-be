package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.DiagnosticArtifactRecord;
import com.youbi.monitor.repository.DiagnosticArtifactRepository;
import com.youbi.monitor.repository.IDiagnosticArtifactRepositoryService;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class DiagnosticArtifactRepositoryServiceImpl implements IDiagnosticArtifactRepositoryService {
    private static final String OPERATOR_DIAGNOSTIC_TABLE = "operator_diagnostic";

    private final DiagnosticArtifactRepository repository;

    public DiagnosticArtifactRepositoryServiceImpl(DiagnosticArtifactRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
    }

    @Override
    public long countOperatorExecutions(Map<String, String> filters) {
        QueryParts parts = executionWhere(filters);
        Long count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT d.op_id
                    FROM operator_diagnostic d
                    LEFT JOIN operator_task t ON t.op_id = d.op_id
                    %s
                    GROUP BY d.op_id
                ) grouped
                """.formatted(parts.where()),
                Long.class,
                parts.args().toArray()
        );
        return count == null ? 0 : count;
    }

    @Override
    public List<Map<String, Object>> listOperatorExecutions(Map<String, String> filters, int offset, int limit) {
        QueryParts parts = executionWhere(filters);
        List<Object> args = new ArrayList<>(parts.args());
        args.add(Math.max(1, limit));
        args.add(Math.max(0, offset));
        return repository.queryForList("""
                SELECT
                    d.op_id AS opId,
                    MAX(NULLIF(t.task_id, '')) AS taskId,
                    MAX(NULLIF(t.account_key, '')) AS accountKey,
                    COUNT(*) AS diagnosticCount,
                    COALESCE(MIN(t.created_at), MIN(d.created_at)) AS createdAt,
                    COALESCE(MIN(t.started_at), MIN(d.created_at)) AS startedAt,
                    COALESCE(MAX(t.completed_at), MAX(d.updated_at)) AS completedAt,
                    COALESCE(
                        MAX(NULLIF(t.platform, '')),
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(MIN(COALESCE(d.screenshot_url, d.html_url, '')), '/diagnostics/', -1),
                            '/',
                            1
                        )
                    ) AS platform,
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(MIN(COALESCE(d.screenshot_url, d.html_url, '')), '/diagnostics/', -1),
                        '/',
                        2
                    ) AS diagnosticPath,
                    MAX(NULLIF(t.action, '')) AS taskAction,
                    CASE
                        WHEN SUM(CASE WHEN t.status = 'running' THEN 1 ELSE 0 END) > 0 THEN 'running'
                        WHEN SUM(CASE WHEN t.status = 'ready' THEN 1 ELSE 0 END) > 0 THEN 'ready'
                        WHEN SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) > 0 THEN 'failed'
                        WHEN SUM(CASE WHEN d.error_message IS NOT NULL AND d.error_message <> '' THEN 1 ELSE 0 END) > 0 THEN 'failed'
                        WHEN SUM(CASE WHEN d.status IS NOT NULL AND d.status NOT IN ('uploaded', 'success') THEN 1 ELSE 0 END) > 0 THEN 'failed'
                        ELSE 'success'
                    END AS status,
                    COALESCE(MAX(NULLIF(t.error_message, '')), MAX(NULLIF(d.error_message, ''))) AS errorMessage
                FROM operator_diagnostic d
                LEFT JOIN operator_task t ON t.op_id = d.op_id
                %s
                GROUP BY d.op_id
                ORDER BY completedAt DESC, opId DESC
                LIMIT ? OFFSET ?
                """.formatted(parts.where()), args.toArray());
    }

    @Override
    public long countOperatorQueue(Map<String, String> filters) {
        QueryParts parts = taskWhere(filters);
        Long count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM operator_task t
                %s
                """.formatted(parts.where()),
                Long.class,
                parts.args().toArray()
        );
        return count == null ? 0 : count;
    }

    @Override
    public List<Map<String, Object>> listOperatorQueue(Map<String, String> filters, int offset, int limit) {
        QueryParts parts = taskWhere(filters);
        List<Object> args = new ArrayList<>(parts.args());
        args.add(Math.max(1, limit));
        args.add(Math.max(0, offset));
        return repository.queryForList("""
                SELECT
                    t.id,
                    t.op_id AS opId,
                    t.run_id AS runId,
                    t.task_id AS taskId,
                    t.platform,
                    t.action,
                    t.task_type AS taskType,
                    COALESCE(NULLIF(type.note, ''), type.display_name) AS taskTypeDisplayName,
                    t.account_key AS accountKey,
                    t.status,
                    t.priority,
                    t.created_at AS createdAt,
                    t.started_at AS startedAt,
                    t.completed_at AS completedAt,
                    t.error_code AS errorCode,
                    t.error_message AS errorMessage
                FROM operator_task t
                LEFT JOIN operator_task_type type ON type.task_type = t.task_type
                %s
                ORDER BY
                    CASE
                        WHEN t.status = 'ready' THEN 2
                        WHEN t.status = 'running' THEN 1
                        ELSE 0
                    END ASC,
                    CASE WHEN t.status = 'ready' THEN t.priority ELSE 0 END DESC,
                    t.created_at ASC,
                    t.id ASC
                LIMIT ? OFFSET ?
                """.formatted(parts.where()), args.toArray());
    }

    @Override
    public long countOperatorDiagnostics(String opId) {
        Long count = repository.queryForObject(
                "SELECT COUNT(*) FROM " + OPERATOR_DIAGNOSTIC_TABLE + " WHERE op_id = ?",
                Long.class,
                text(opId)
        );
        return count == null ? 0 : count;
    }

    @Override
    public List<DiagnosticArtifactRecord> listOperatorDiagnostics(String opId, int offset, int limit) {
        return repository.query("""
                SELECT *
                FROM operator_diagnostic
                WHERE op_id = ?
                ORDER BY step_index ASC, id ASC
                LIMIT ? OFFSET ?
                """, this::mapDiagnostic, text(opId), Math.max(1, limit), Math.max(0, offset));
    }

    @Override
    public DiagnosticArtifactRecord getOperatorDiagnostic(long id) {
        List<DiagnosticArtifactRecord> rows = repository.query("""
                SELECT *
                FROM operator_diagnostic
                WHERE id = ?
                LIMIT 1
                """, this::mapDiagnostic, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private QueryParts executionWhere(Map<String, String> filters) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        addLike(conditions, args, "d.op_id", filters.get("opId"));
        addLike(conditions, args, "t.task_id", filters.get("taskId"));
        addLike(conditions, args, "t.account_key", filters.get("accountKey"));
        addPlatformFilter(conditions, args, filters.get("platform"));
        addLike(conditions, args, "COALESCE(t.action, d.screenshot_url, d.html_url, '')", filters.get("action"));
        addDateLowerBound(conditions, args, "COALESCE(t.created_at, d.created_at)", filters.get("createdFrom"));
        addDateUpperBound(conditions, args, "COALESCE(t.created_at, d.created_at)", filters.get("createdTo"));
        String status = text(filters.get("status"));
        if (!status.isBlank()) {
            if ("ready".equals(status) || "running".equals(status)) {
                conditions.add("t.status = ?");
                args.add(status);
            } else if ("failed".equals(status)) {
                conditions.add("(t.status = 'failed' OR d.error_message IS NOT NULL AND d.error_message <> '' OR d.status NOT IN ('uploaded', 'success'))");
            } else if ("success".equals(status)) {
                conditions.add("(t.status IS NULL OR t.status = 'success')");
                conditions.add("(d.error_message IS NULL OR d.error_message = '')");
                conditions.add("(d.status IS NULL OR d.status IN ('uploaded', 'success'))");
            }
        }
        String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
        return new QueryParts(where, args);
    }

    private QueryParts taskWhere(Map<String, String> filters) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        addLike(conditions, args, "t.op_id", filters.get("opId"));
        addLike(conditions, args, "t.task_id", filters.get("taskId"));
        addLike(conditions, args, "t.account_key", filters.get("accountKey"));
        addLike(conditions, args, "COALESCE(t.task_type, t.action, '')", filters.get("action"));
        String platform = text(filters.get("platform"));
        if (!platform.isBlank()) {
            conditions.add("t.platform = ?");
            args.add(platform);
        }
        String status = text(filters.get("status"));
        if (!status.isBlank()) {
            conditions.add("t.status = ?");
            args.add(status);
        }
        addDateLowerBound(conditions, args, "t.created_at", filters.get("createdFrom"));
        addDateUpperBound(conditions, args, "t.created_at", filters.get("createdTo"));
        String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
        return new QueryParts(where, args);
    }

    private void addPlatformFilter(List<String> conditions, List<Object> args, String value) {
        String text = text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add("(t.platform = ? OR COALESCE(d.screenshot_url, d.html_url, '') LIKE ?)");
        args.add(text);
        args.add("%/diagnostics/" + text + "/%");
    }

    private void addLike(List<String> conditions, List<Object> args, String column, String value) {
        String text = text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " LIKE ?");
        args.add("%" + text + "%");
    }

    private void addDateLowerBound(List<String> conditions, List<Object> args, String column, String value) {
        String text = text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " >= ?");
        args.add(text);
    }

    private void addDateUpperBound(List<String> conditions, List<Object> args, String column, String value) {
        String text = text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " < ?");
        args.add(text);
    }

    private DiagnosticArtifactRecord mapDiagnostic(ResultSet rs, int rowNum) throws SQLException {
        return new DiagnosticArtifactRecord(
                rs.getLong("id"),
                rs.getString("op_id"),
                rs.getString("op_id"),
                null,
                null,
                null,
                null,
                null,
                rs.getInt("step_index"),
                rs.getString("step_name"),
                rs.getString("screenshot_url"),
                rs.getString("html_url"),
                nullableLong(rs, "screenshot_size_bytes"),
                nullableLong(rs, "html_size_bytes"),
                nullableInt(rs, "screenshot_width"),
                nullableInt(rs, "screenshot_height"),
                rs.getString("status"),
                rs.getString("error_message"),
                timestamp(rs, "created_at")
        );
    }

    private Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private LocalDateTime timestamp(ResultSet rs, String column) throws SQLException {
        java.sql.Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record QueryParts(String where, List<Object> args) {
    }
}
