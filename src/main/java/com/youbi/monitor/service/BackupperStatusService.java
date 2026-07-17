package com.youbi.monitor.service;

import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class BackupperStatusService {
    private final MonitorRepository repository;
    private final AccountSchemaService schemaService;

    public BackupperStatusService(MonitorRepository repository, AccountSchemaService schemaService) {
        this.repository = repository;
        this.schemaService = schemaService;
    }

    public BackupperStatus latestBackupperStatus() {
        if (!schemaService.tableExists("backupper_status")) {
            return null;
        }
        List<BackupperStatus> rows = repository.query(
                """
                SELECT id, host, device, mount_point, total_gb, used_gb, available_gb,
                       used_percent, total_label, minio_bytes, minio_ydbi_bytes,
                       minio_diagnostics_bytes, docker_image_bytes, docker_dangling_image_bytes,
                       docker_build_cache_bytes, workfolder_bytes, mysql_bytes, mysql_binlog_bytes, created_at
                FROM backupper_status
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (rs, rowNum) -> new BackupperStatus(
                        rs.getLong("id"),
                        rs.getString("host"),
                        rs.getString("device"),
                        rs.getString("mount_point"),
                        rs.getBigDecimal("total_gb"),
                        rs.getBigDecimal("used_gb"),
                        rs.getBigDecimal("available_gb"),
                        rs.getBigDecimal("used_percent"),
                        formatBackupperStatusText(rs.getBigDecimal("used_gb"), rs.getString("total_label")),
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
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void ensureBackupperStatusStorageColumns() {
        ensureBackupperStatusColumn("minio_bytes");
        ensureBackupperStatusColumn("minio_ydbi_bytes");
        ensureBackupperStatusColumn("minio_diagnostics_bytes");
        ensureBackupperStatusColumn("docker_image_bytes");
        ensureBackupperStatusColumn("docker_dangling_image_bytes");
        ensureBackupperStatusColumn("docker_build_cache_bytes");
        ensureBackupperStatusColumn("workfolder_bytes");
        ensureBackupperStatusColumn("mysql_bytes");
        ensureBackupperStatusColumn("mysql_binlog_bytes");
    }

    private void ensureBackupperStatusColumn(String column) {
        if (!schemaService.columnExists("backupper_status", column)) {
            repository.update(
                    "ALTER TABLE backupper_status ADD COLUMN " + column + " BIGINT UNSIGNED NULL"
            );
        }
    }

    private static String formatBackupperStatusText(BigDecimal usedGb, String totalLabel) {
        if (usedGb == null) {
            return "";
        }
        String total = totalLabel == null ? "" : totalLabel.trim();
        if (total.isBlank()) {
            return oneDecimal(usedGb) + "G";
        }
        return oneDecimal(usedGb) + "G/" + total;
    }

    private static String oneDecimal(BigDecimal value) {
        return value.setScale(1, java.math.RoundingMode.HALF_UP).toPlainString();
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static Long nullableLong(ResultSet rs, String column) throws java.sql.SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }
}
