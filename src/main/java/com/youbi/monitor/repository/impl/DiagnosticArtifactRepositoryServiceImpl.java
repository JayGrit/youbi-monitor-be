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
                "SELECT COUNT(*) FROM (SELECT op_id FROM " + OPERATOR_DIAGNOSTIC_TABLE + parts.where() + " GROUP BY op_id) t",
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
                    op_id AS opId,
                    COUNT(*) AS diagnosticCount,
                    MIN(created_at) AS createdAt,
                    MIN(created_at) AS startedAt,
                    MAX(updated_at) AS completedAt,
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(MIN(COALESCE(screenshot_url, html_url, '')), '/diagnostics/', -1),
                        '/',
                        1
                    ) AS platform,
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(MIN(COALESCE(screenshot_url, html_url, '')), '/diagnostics/', -1),
                        '/',
                        2
                    ) AS diagnosticPath,
                    CASE
                        WHEN SUM(CASE WHEN error_message IS NOT NULL AND error_message <> '' THEN 1 ELSE 0 END) > 0 THEN 'failed'
                        WHEN SUM(CASE WHEN status IS NOT NULL AND status NOT IN ('uploaded', 'success') THEN 1 ELSE 0 END) > 0 THEN 'failed'
                        ELSE 'success'
                    END AS status,
                    MAX(NULLIF(error_message, '')) AS errorMessage
                FROM operator_diagnostic
                %s
                GROUP BY op_id
                ORDER BY completedAt DESC, opId DESC
                LIMIT ? OFFSET ?
                """.formatted(parts.where()), args.toArray());
    }

    @Override
    public List<DiagnosticArtifactRecord> listOperatorDiagnostics(String opId) {
        return repository.query("""
                SELECT *
                FROM operator_diagnostic
                WHERE op_id = ?
                ORDER BY step_index ASC, id ASC
                """, this::mapDiagnostic, opId);
    }

    private QueryParts executionWhere(Map<String, String> filters) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        addLike(conditions, args, "op_id", filters.get("opId"));
        addLike(conditions, args, "op_id", filters.get("taskId"));
        addLike(conditions, args, "COALESCE(screenshot_url, html_url, '')", filters.get("platform"));
        addLike(conditions, args, "COALESCE(screenshot_url, html_url, '')", filters.get("action"));
        String status = text(filters.get("status"));
        if (!status.isBlank()) {
            if ("failed".equals(status)) {
                conditions.add("(error_message IS NOT NULL AND error_message <> '' OR status NOT IN ('uploaded', 'success'))");
            } else if ("success".equals(status)) {
                conditions.add("(error_message IS NULL OR error_message = '')");
                conditions.add("(status IS NULL OR status IN ('uploaded', 'success'))");
            }
        }
        String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
        return new QueryParts(where, args);
    }

    private void addLike(List<String> conditions, List<Object> args, String column, String value) {
        String text = text(value);
        if (text.isBlank()) {
            return;
        }
        conditions.add(column + " LIKE ?");
        args.add("%" + text + "%");
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
