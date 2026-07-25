package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.DeviceHeartbeat;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
class ServiceHeartbeatQueryService extends MonitorRepositorySqlSupport {
    private static final long MIN_HEARTBEAT_ONLINE_SECONDS = 90;
    private static final String HEARTBEAT_TABLE = "service_instance_heartbeat";
    private static final String HEARTBEAT_SQL = """
            SELECT service_name,
                   instance_id,
                   device_name,
                   host_name,
                   process_id,
                   runtime_role,
                   status,
                   last_seen_at,
                   heartbeat_interval_seconds
            FROM service_instance_heartbeat
            ORDER BY service_name, device_name, host_name, process_id, instance_id
            """;

    ServiceHeartbeatQueryService(MonitorRepository repository, TaskFlowRowQueryService rowQueryService) {
        super(repository);
    }

    List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now) {
        Map<String, List<DeviceHeartbeat>> byService = new LinkedHashMap<>();
        for (StageDefinition stage : STAGES) {
            byService.put(stage.key(), new ArrayList<>());
        }

        repository.query(HEARTBEAT_SQL, rs -> {
            String serviceName = rs.getString("service_name");
            if (!byService.containsKey(serviceName)) {
                return;
            }
            byService.get(serviceName).add(deviceHeartbeat(rs, now));
        });

        List<ServiceHeartbeat> result = new ArrayList<>();
        for (StageDefinition stage : STAGES) {
            result.add(new ServiceHeartbeat(stage.key(), stage.label(), byService.get(stage.key())));
        }
        return result;
    }

    private static DeviceHeartbeat deviceHeartbeat(ResultSet rs, LocalDateTime now) throws SQLException {
        LocalDateTime lastSeenAt = timestamp(rs, "last_seen_at");
        long secondsSinceLastSeen = elapsedSeconds(lastSeenAt, null, now);
        int intervalSeconds = Math.max(1, rs.getInt("heartbeat_interval_seconds"));
        long onlineSeconds = Math.max(MIN_HEARTBEAT_ONLINE_SECONDS, intervalSeconds * 3L);
        boolean online = lastSeenAt != null && secondsSinceLastSeen <= onlineSeconds;
        return new DeviceHeartbeat(
                rs.getString("device_name"),
                lastSeenAt,
                online,
                secondsSinceLastSeen,
                rs.getString("instance_id"),
                rs.getString("host_name"),
                nullableInt(rs, "process_id"),
                rs.getString("runtime_role"),
                rs.getString("status")
        );
    }

    private static Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }
}
