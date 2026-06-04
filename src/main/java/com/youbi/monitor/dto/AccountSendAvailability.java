package com.youbi.monitor.dto;

import java.time.LocalDateTime;

public record AccountSendAvailability(
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        int todayUploadCount,
        int cooldownWaitingCount,
        String uploadRunningTaskId,
        int uploadRunningCount
) {
}
