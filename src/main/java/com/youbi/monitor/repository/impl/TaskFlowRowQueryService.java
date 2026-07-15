package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
class TaskFlowRowQueryService extends MonitorRepositorySqlSupport {
    private final Map<String, List<String>> tableColumnsCache = new ConcurrentHashMap<>();

    TaskFlowRowQueryService(MonitorRepository repository) {
        super(repository);
    }

    Map<String, Object> findTaskFlowRow(String table, String idColumn, String id) {
        if (table == null || !tableExists(table)) {
            return Map.of();
        }
        List<Map<String, Object>> rows = listTaskFlowRows(table, idColumn, id, idColumn, 1);
        return rows.isEmpty() ? Map.of() : rows.get(0);
    }

    List<Map<String, Object>> listTaskFlowRows(String table, String idColumn, String id, String orderBy, int limit) {
        return repository.query(
                "SELECT * FROM " + quotedIdentifier(table)
                        + " WHERE " + quotedIdentifier(idColumn) + " = ?"
                        + orderClause(table, orderBy)
                        + " LIMIT ?",
                (rs, rowNum) -> rowMap(rs),
                id,
                limit
        );
    }

    private String orderClause(String table, String orderBy) {
        if (orderBy == null || orderBy.isBlank()) {
            return "";
        }
        Set<String> columns = new HashSet<>(columns(table));
        List<String> parts = new ArrayList<>();
        for (String raw : orderBy.split(",")) {
            String column = raw.trim();
            if (columns.contains(column)) {
                parts.add(quotedIdentifier(column));
            }
        }
        return parts.isEmpty() ? "" : " ORDER BY " + String.join(", ", parts);
    }

    List<String> columns(String table) {
        if (!tableExists(table)) {
            return List.of();
        }
        return tableColumnsCache.computeIfAbsent(table, key -> repository.queryForList("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """, String.class, key));
    }

    private Map<String, Object> rowMap(ResultSet rs) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        int count = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= count; i++) {
            String name = rs.getMetaData().getColumnLabel(i);
            Object value = rs.getObject(i);
            if (value instanceof Timestamp timestamp) {
                value = timestamp.toLocalDateTime();
            }
            row.put(name, value);
        }
        return row;
    }
}
