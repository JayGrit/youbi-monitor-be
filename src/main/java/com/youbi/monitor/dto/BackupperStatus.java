package com.youbi.monitor.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record BackupperStatus(
        Long id,
        String host,
        String device,
        String mountPoint,
        BigDecimal totalGb,
        BigDecimal usedGb,
        BigDecimal availableGb,
        BigDecimal usedPercent,
        String statusText,
        Long minioBytes,
        Long minioYdbiBytes,
        Long minioDiagnosticsBytes,
        Long dockerImageBytes,
        Long dockerDanglingImageBytes,
        Long dockerBuildCacheBytes,
        Long workfolderBytes,
        Long mysqlBytes,
        LocalDateTime createdAt
) {
}
