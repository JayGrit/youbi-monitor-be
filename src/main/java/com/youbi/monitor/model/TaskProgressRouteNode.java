package com.youbi.monitor.model;

import java.time.LocalDateTime;
import java.util.List;

public record TaskProgressRouteNode(
        String id,
        String stage,
        String subStage,
        String label,
        int order,
        String status,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        long elapsedSeconds,
        Integer completedCount,
        Integer failedCount,
        Integer totalCount,
        Double progressPercent,
        String errorMessage,
        String childErrorMessage,
        List<UploadPlatformStatus> platformStatuses,
        List<StageError> errors,
        int errorCount,
        boolean errorsTruncated
) {
}
