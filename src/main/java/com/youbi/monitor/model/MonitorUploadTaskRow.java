package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record MonitorUploadTaskRow(
        String uploadTaskId,
        String platform,
        String upstreamTaskId,
        String accountKey,
        String status,
        String resultJson,
        String errorCode,
        String errorMessage,
        LocalDateTime startedAt,
        LocalDateTime completedAt
) {
}
