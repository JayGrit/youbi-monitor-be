package com.youbi.monitor;

import java.time.LocalDateTime;

public record AccountSendAvailability(
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        int todayUploadCount,
        int cooldownWaitingCount
) {
}
