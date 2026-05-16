package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.List;

public record TaskMonitorItem(
        String taskId,
        String title,
        String sourceUrl,
        String status,
        String currentStage,
        LocalDateTime createdAt,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        long elapsedSeconds,
        String bilibiliUploadUid,
        String bilibiliUploadAccountName,
        String errorMessage,
        List<StageNode> nodes
) {
}
