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
        if ("task_info".equals(table) && "task_id".equals(idColumn)) {
            List<Map<String, Object>> rows = repository.query("""
                    SELECT
                      t.id AS task_id,
                      t.submitter_video_id,
                      t.topic,
                      t.task_type,
                      ts.source_url,
                      ts.source_thumbnail_url,
                      ts.source_subtitle_txt_url,
                      ts.metadata_url,
                      ts.video_source_url,
                      ts.audio_source_url,
                      opts.target_language,
                      proc.audio_vocals_url,
                      proc.audio_bgm_url,
                      proc.audio_dubbing_url,
                      proc.tts_segments_dir,
                      proc.asr_json_path,
                      proc.translation_json_path,
                      proc.timings_json_path,
                      proc.source_transcript_txt_url,
                      meta.upload_title,
                      meta.upload_description,
                      meta.upload_tags,
                      meta.cover_text,
                      meta.final_cover_url,
                      meta.cover_4_3,
                      meta.cover_3_4,
                      meta.final_video_url,
                      ts.source_title AS title,
                      ts.source_description,
                      ts.source_uploader,
                      ts.source_webpage_url,
                      ts.source_tags_json,
                      ts.source_duration_seconds
                    FROM task t
                    LEFT JOIN task_source ts ON ts.task_id = t.id
                    LEFT JOIN task_options opts ON opts.task_id = t.id
                    LEFT JOIN task_processing proc ON proc.task_id = t.id
                    LEFT JOIN task_metadata meta ON meta.task_id = t.id
                    WHERE t.id = ?
                    LIMIT 1
                    """, (rs, rowNum) -> rowMap(rs), id);
            return rows.isEmpty() ? Map.of() : rows.get(0);
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
