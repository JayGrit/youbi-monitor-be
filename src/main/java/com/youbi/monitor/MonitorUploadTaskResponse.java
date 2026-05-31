package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.Map;

public record MonitorUploadTaskResponse(
        boolean accepted,
        String uploadTaskId,
        String platform,
        String taskId,
        String accountKey,
        String status,
        Boolean success,
        String rejectReason,
        String errorCode,
        String message,
        Long durationMs,
        String videoUrl,
        String bvid,
        Long aid,
        Long accountUid,
        String accountName,
        Map<String, Object> raw,
        LocalDateTime startedAt,
        LocalDateTime completedAt
) {
}
