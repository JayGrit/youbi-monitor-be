package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record TaskMonitorSummary(
        String taskId,
        String title,
        String sourceUrl,
        String sourceWebpageUrl,
        String sourceThumbnailUrl,
        Double sourceDurationSeconds,
        Long minioStorageBytes,
        Integer minioStorageObjectCount,
        LocalDateTime minioStorageUpdatedAt,
        String taskType,
        String status,
        String currentStage,
        LocalDateTime createdAt,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        long elapsedSeconds,
        String errorMessage
) {
}
