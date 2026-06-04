package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record StageNode(
        String key,
        String label,
        String status,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        long elapsedSeconds,
        Integer completedCount,
        Integer failedCount,
        Integer totalCount,
        String errorMessage,
        String childErrorMessage
) {
}
