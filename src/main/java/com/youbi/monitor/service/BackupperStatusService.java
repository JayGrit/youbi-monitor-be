package com.youbi.monitor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class BackupperStatusService {
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final MonitorRepository repository;
    private final AccountSchemaService schemaService;
    private final ObjectMapper objectMapper;

    public BackupperStatusService(
            MonitorRepository repository,
            AccountSchemaService schemaService,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.schemaService = schemaService;
        this.objectMapper = objectMapper;
    }

    public BackupperStatus latestBackupperStatus() {
        if (!schemaService.tableExists("backupper_status_entry")) {
            return legacyBackupperStatus();
        }
        List<BackupperStatus.Component> rows = repository.query(
                """
                SELECT entry.id, entry.host, entry.component, entry.status,
                       entry.payload_json, entry.error_message, entry.created_at
                FROM backupper_status_entry entry
                INNER JOIN (
                    SELECT component, MAX(id) AS id
                    FROM backupper_status_entry
                    GROUP BY component
                ) latest
                    ON latest.component = entry.component
                   AND latest.id = entry.id
                ORDER BY entry.component ASC
                """,
                (rs, rowNum) -> new BackupperStatus.Component(
                        rs.getLong("id"),
                        rs.getString("host"),
                        rs.getString("component"),
                        rs.getString("status"),
                        parsePayload(rs.getString("payload_json")),
                        rs.getString("error_message"),
                        toLocalDateTime(rs.getTimestamp("created_at"))
                )
        );
        if (rows.isEmpty()) {
            return legacyBackupperStatus();
        }

        Map<String, BackupperStatus.Component> components = new LinkedHashMap<>();
        String host = null;
        LocalDateTime createdAt = null;
        for (BackupperStatus.Component row : rows) {
            String component = componentName(row);
            if (component.isBlank()) {
                continue;
            }
            components.put(component, row);
            if (host == null || host.isBlank()) {
                host = row.host();
            }
            if (row.createdAt() != null && (createdAt == null || row.createdAt().isAfter(createdAt))) {
                createdAt = row.createdAt();
            }
        }
        if (components.isEmpty()) {
            return null;
        }
        return new BackupperStatus(host, components, summary(components), createdAt);
    }

    private BackupperStatus legacyBackupperStatus() {
        if (!schemaService.tableExists("backupper_status")) {
            return null;
        }
        List<LegacyStatus> rows = repository.query(
                """
                SELECT id, host, device, mount_point, total_gb, used_gb, available_gb,
                       used_percent, status_text, minio_bytes, minio_ydbi_bytes,
                       minio_diagnostics_bytes, docker_image_bytes, docker_dangling_image_bytes,
                       docker_build_cache_bytes, workfolder_bytes, mysql_bytes, mysql_binlog_bytes, created_at
                FROM backupper_status
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (rs, rowNum) -> new LegacyStatus(
                        rs.getLong("id"),
                        rs.getString("host"),
                        payloadMap(
                                "device", rs.getString("device"),
                                "mountPoint", rs.getString("mount_point"),
                                "totalGb", rs.getBigDecimal("total_gb"),
                                "usedGb", rs.getBigDecimal("used_gb"),
                                "availableGb", rs.getBigDecimal("available_gb"),
                                "usedPercent", rs.getBigDecimal("used_percent"),
                                "statusText", rs.getString("status_text")
                        ),
                        nullableLong(rs, "minio_bytes"),
                        nullableLong(rs, "minio_ydbi_bytes"),
                        nullableLong(rs, "minio_diagnostics_bytes"),
                        nullableLong(rs, "docker_image_bytes"),
                        nullableLong(rs, "docker_dangling_image_bytes"),
                        nullableLong(rs, "docker_build_cache_bytes"),
                        nullableLong(rs, "workfolder_bytes"),
                        nullableLong(rs, "mysql_bytes"),
                        nullableLong(rs, "mysql_binlog_bytes"),
                        toLocalDateTime(rs.getTimestamp("created_at"))
                )
        );
        if (rows.isEmpty()) {
            return null;
        }
        LegacyStatus legacy = rows.get(0);
        Map<String, BackupperStatus.Component> components = new LinkedHashMap<>();
        components.put("disk", legacyComponent(legacy, "disk", legacy.disk()));
        components.put("minio", legacyComponent(legacy, "minio", payloadMap(
                "totalBytes", legacy.minioBytes(),
                "bucketBytes", payloadMap(
                        "ydbi", legacy.minioYdbiBytes(),
                        "youbi-diagnostics", legacy.minioDiagnosticsBytes()
                )
        )));
        components.put("docker", legacyComponent(legacy, "docker", payloadMap(
                "imageBytes", legacy.dockerImageBytes(),
                "danglingImageBytes", legacy.dockerDanglingImageBytes(),
                "buildCacheBytes", legacy.dockerBuildCacheBytes()
        )));
        components.put("workfolder", legacyComponent(legacy, "workfolder", payloadMap("bytes", legacy.workfolderBytes())));
        components.put("mysql", legacyComponent(legacy, "mysql", payloadMap("bytes", legacy.mysqlBytes())));
        components.put("mysql_binlog", legacyComponent(legacy, "mysql_binlog", payloadMap("bytes", legacy.mysqlBinlogBytes())));
        return new BackupperStatus(legacy.host(), components, summary(components), legacy.createdAt());
    }

    private Map<String, Object> parsePayload(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(payloadJson, PAYLOAD_TYPE);
        } catch (JsonProcessingException ignored) {
            return Map.of();
        }
    }

    private static String componentName(BackupperStatus.Component component) {
        return component.component() == null ? "" : component.component().trim();
    }

    private static Map<String, Object> summary(Map<String, BackupperStatus.Component> components) {
        Map<String, Object> summary = new LinkedHashMap<>();
        Map<String, Object> disk = payload(components, "disk");
        Map<String, Object> minio = payload(components, "minio");
        Map<String, Object> docker = payload(components, "docker");
        Map<String, Object> workfolder = payload(components, "workfolder");
        Map<String, Object> mysql = payload(components, "mysql");
        Map<String, Object> mysqlBinlog = payload(components, "mysql_binlog");
        summary.put("disk", disk);
        summary.put("minioBytes", minio.get("totalBytes"));
        summary.put("minioBucketBytes", minio.getOrDefault("bucketBytes", Map.of()));
        summary.put("docker", docker);
        summary.put("workfolderBytes", workfolder.get("bytes"));
        summary.put("mysqlBytes", mysql.get("bytes"));
        summary.put("mysqlBinlogBytes", mysqlBinlog.get("bytes"));
        return summary;
    }

    private static Map<String, Object> payload(
            Map<String, BackupperStatus.Component> components,
            String component
    ) {
        BackupperStatus.Component row = components.get(component);
        return row == null || row.payload() == null ? Map.of() : row.payload();
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static BackupperStatus.Component legacyComponent(
            LegacyStatus legacy,
            String component,
            Map<String, Object> payload
    ) {
        return new BackupperStatus.Component(
                legacy.id(),
                legacy.host(),
                component,
                "success",
                payload,
                null,
                legacy.createdAt()
        );
    }

    private static Long nullableLong(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Map<String, Object> payloadMap(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index + 1 < pairs.length; index += 2) {
            values.put(String.valueOf(pairs[index]), pairs[index + 1]);
        }
        return values;
    }

    private record LegacyStatus(
            Long id,
            String host,
            Map<String, Object> disk,
            Long minioBytes,
            Long minioYdbiBytes,
            Long minioDiagnosticsBytes,
            Long dockerImageBytes,
            Long dockerDanglingImageBytes,
            Long dockerBuildCacheBytes,
            Long workfolderBytes,
            Long mysqlBytes,
            Long mysqlBinlogBytes,
            LocalDateTime createdAt
    ) {
    }
}
