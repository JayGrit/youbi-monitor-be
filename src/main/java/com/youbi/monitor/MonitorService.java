package com.youbi.monitor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class MonitorService {
    private static final long HEARTBEAT_ONLINE_SECONDS = 60;
    private static final List<String> HEARTBEAT_DEVICES = List.of(
            "Macbook Air M4",
            "Macmini M2",
            "LPXB",
            "MY_HP",
            "LPXB_HP",
            "TXY"
    );
    private static final List<StageDefinition> STAGES = List.of(
            new StageDefinition("downloader", "下载", "downloader_status", "downloader_started_at", "downloader_completed_at", "downloader_error"),
            new StageDefinition("demucs", "人声分离", "demucs_status", "demucs_started_at", "demucs_completed_at", "demucs_error"),
            new StageDefinition("whisper", "语音识别", "whisper_status", "whisper_started_at", "whisper_completed_at", "whisper_error"),
            new StageDefinition("translator", "翻译", "translator_status", "translator_started_at", "translator_completed_at", "translator_error"),
            new StageDefinition("speaker", "配音", "speaker_status", "speaker_started_at", "speaker_completed_at", "speaker_error"),
            new StageDefinition("combiner", "音视频合成", "combiner_status", "combiner_started_at", "combiner_completed_at", "combiner_error"),
            new StageDefinition("uploader", "上传", "uploader_status", "uploader_started_at", "uploader_completed_at", "uploader_error")
    );

    private static final String MONITOR_SQL = """
            SELECT
              t.id,
              t.title,
              t.source_url,
              t.status,
              t.current_stage,
              t.created_at,
              t.started_at,
              t.completed_at,
              t.error_message,

              d.status downloader_status,
              d.started_at downloader_started_at,
              d.completed_at downloader_completed_at,
              d.error_message downloader_error,

              de.status demucs_status,
              de.started_at demucs_started_at,
              de.completed_at demucs_completed_at,
              de.error_message demucs_error,

              w.status whisper_status,
              w.started_at whisper_started_at,
              w.completed_at whisper_completed_at,
              w.error_message whisper_error,

              tr.status translator_status,
              tr.started_at translator_started_at,
              tr.completed_at translator_completed_at,
              tr.error_message translator_error,
              ts.translated_count translator_completed_count,
              GREATEST(COALESCE(fa.fixed_count, 0), COALESCE(ts.translated_count, 0)) translator_total_count,

              sp.status speaker_status,
              sp.started_at speaker_started_at,
              sp.completed_at speaker_completed_at,
              sp.error_message speaker_error,
              ss.speaker_completed_count,
              ss.speaker_total_count,

              m.status combiner_status,
              m.started_at combiner_started_at,
              m.completed_at combiner_completed_at,
              m.error_message combiner_error,

              u.status uploader_status,
              u.started_at uploader_started_at,
              u.completed_at uploader_completed_at,
              u.error_message uploader_error
            FROM yd_task t
            LEFT JOIN yd_downloader d ON d.task_id = t.id
            LEFT JOIN yd_demucs de ON de.task_id = t.id
            LEFT JOIN yd_whisper w ON w.task_id = t.id
            LEFT JOIN yd_translator tr ON tr.task_id = t.id
            LEFT JOIN yd_speaker sp ON sp.task_id = t.id
            LEFT JOIN yd_combiner m ON m.task_id = t.id
            LEFT JOIN yd_uploader u ON u.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) fixed_count
              FROM yd_asr_segment
              WHERE segment_type = 'fixed'
              GROUP BY task_id
            ) fa ON fa.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) translated_count
              FROM yd_speaker_segment
              GROUP BY task_id
            ) ts ON ts.task_id = t.id
            LEFT JOIN (
              SELECT
                task_id,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) speaker_completed_count,
                COUNT(*) speaker_total_count
              FROM yd_speaker_segment
              GROUP BY task_id
            ) ss ON ss.task_id = t.id
            ORDER BY t.created_at DESC
            LIMIT ?
            """;
    private static final String HEARTBEAT_TABLE = "yd_service_heartbeat";
    private static final String HEARTBEAT_TABLE_EXISTS_SQL = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
            """;
    private static final String HEARTBEAT_COLUMNS_SQL = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
            """;

    private final JdbcTemplate jdbcTemplate;

    public MonitorService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public MonitorResponse listTasks(int limit) {
        LocalDateTime now = LocalDateTime.now();
        List<TaskMonitorItem> tasks = jdbcTemplate.query(MONITOR_SQL, new TaskRowMapper(now), limit);
        List<ServiceHeartbeat> serviceHeartbeats = listServiceHeartbeats(now);
        return new MonitorResponse(tasks, serviceHeartbeats, now);
    }

    private List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now) {
        Map<String, ServiceHeartbeat> byService = new LinkedHashMap<>();
        for (StageDefinition stage : STAGES) {
            byService.put(stage.key(), emptyHeartbeat(stage));
        }

        if (!heartbeatTableExists()) {
            return new ArrayList<>(byService.values());
        }

        jdbcTemplate.query(heartbeatSql(), rs -> {
            String serviceName = rs.getString("service_name");
            String label = labelForService(serviceName);
            byService.put(serviceName, new ServiceHeartbeat(serviceName, label, deviceHeartbeats(rs, now)));
        });
        return new ArrayList<>(byService.values());
    }

    private boolean heartbeatTableExists() {
        Integer count = jdbcTemplate.queryForObject(HEARTBEAT_TABLE_EXISTS_SQL, Integer.class, HEARTBEAT_TABLE);
        return count != null && count > 0;
    }

    private String heartbeatSql() {
        List<String> columns = jdbcTemplate.queryForList(HEARTBEAT_COLUMNS_SQL, String.class, HEARTBEAT_TABLE);
        StringBuilder sql = new StringBuilder("SELECT service_name");
        for (String device : HEARTBEAT_DEVICES) {
            sql.append(",\n  ");
            if (columns.contains(device)) {
                sql.append(quotedIdentifier(device));
            } else {
                sql.append("NULL AS ").append(quotedIdentifier(device));
            }
        }
        sql.append("\nFROM ").append(HEARTBEAT_TABLE);
        return sql.toString();
    }

    private static String quotedIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static ServiceHeartbeat emptyHeartbeat(StageDefinition stage) {
        return new ServiceHeartbeat(stage.key(), stage.label(), emptyDeviceHeartbeats());
    }

    private static List<DeviceHeartbeat> emptyDeviceHeartbeats() {
        List<DeviceHeartbeat> devices = new ArrayList<>();
        for (String device : HEARTBEAT_DEVICES) {
            devices.add(new DeviceHeartbeat(device, null, false, 0));
        }
        return devices;
    }

    private static List<DeviceHeartbeat> deviceHeartbeats(ResultSet rs, LocalDateTime now) throws SQLException {
        List<DeviceHeartbeat> devices = new ArrayList<>();
        for (String device : HEARTBEAT_DEVICES) {
            LocalDateTime lastSeenAt = timestamp(rs, device);
            long secondsSinceLastSeen = elapsedSeconds(lastSeenAt, null, now);
            boolean online = lastSeenAt != null && secondsSinceLastSeen <= HEARTBEAT_ONLINE_SECONDS;
            devices.add(new DeviceHeartbeat(device, lastSeenAt, online, secondsSinceLastSeen));
        }
        return devices;
    }

    private static String labelForService(String serviceName) {
        for (StageDefinition stage : STAGES) {
            if (stage.key().equals(serviceName)) {
                return stage.label();
            }
        }
        return serviceName;
    }

    private static LocalDateTime timestamp(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static long elapsedSeconds(LocalDateTime startedAt, LocalDateTime completedAt, LocalDateTime now) {
        if (startedAt == null) {
            return 0;
        }
        LocalDateTime end = completedAt == null ? now : completedAt;
        return Math.max(0, Duration.between(startedAt, end).getSeconds());
    }

    private static class TaskRowMapper implements RowMapper<TaskMonitorItem> {
        private final LocalDateTime now;

        private TaskRowMapper(LocalDateTime now) {
            this.now = now;
        }

        @Override
        public TaskMonitorItem mapRow(ResultSet rs, int rowNum) throws SQLException {
            LocalDateTime taskStartedAt = timestamp(rs, "started_at");
            LocalDateTime taskCompletedAt = timestamp(rs, "completed_at");
            List<StageNode> nodes = new ArrayList<>();
            for (StageDefinition stage : STAGES) {
                LocalDateTime startedAt = timestamp(rs, stage.startedAtColumn());
                LocalDateTime completedAt = timestamp(rs, stage.completedAtColumn());
                nodes.add(new StageNode(
                        stage.key(),
                        stage.label(),
                        stringOrDefault(rs, stage.statusColumn(), "pending"),
                        startedAt,
                        completedAt,
                        elapsedSeconds(startedAt, completedAt, now),
                        countValue(rs, stage.key(), "completed_count"),
                        countValue(rs, stage.key(), "total_count"),
                        rs.getString(stage.errorColumn())
                ));
            }

            return new TaskMonitorItem(
                    rs.getString("id"),
                    rs.getString("title"),
                    rs.getString("source_url"),
                    rs.getString("status"),
                    rs.getString("current_stage"),
                    MonitorService.timestamp(rs, "created_at"),
                    taskStartedAt,
                    taskCompletedAt,
                    MonitorService.elapsedSeconds(taskStartedAt, taskCompletedAt, now),
                    rs.getString("error_message"),
                    nodes
            );
        }

        private static String stringOrDefault(ResultSet rs, String column, String defaultValue) throws SQLException {
            String value = rs.getString(column);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static Integer countValue(ResultSet rs, String stageKey, String suffix) throws SQLException {
            if (!"translator".equals(stageKey) && !"speaker".equals(stageKey)) {
                return null;
            }
            int value = rs.getInt(stageKey + "_" + suffix);
            return rs.wasNull() ? null : value;
        }

    }
}
