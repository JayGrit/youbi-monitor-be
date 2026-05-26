package com.youbi.monitor;

import java.time.LocalDateTime;

public record DouyinCdpSession(
        String accountKey,
        Integer cdpPort,
        String cdpEndpoint,
        String note,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        Integer uploadCooldownMinSeconds,
        Integer uploadCooldownMaxSeconds,
        int todayUploadCount,
        int cooldownWaitingCount,
        boolean enabled,
        LocalDateTime updatedAt
) {
}
