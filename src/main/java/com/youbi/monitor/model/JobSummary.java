package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record JobSummary(
        String serviceName,
        String sourceTable,
        int totalCount,
        int completedCount,
        int failedCount,
        int runningCount,
        int pendingCount,
        int readyCount,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        long elapsedSeconds
) {
}
