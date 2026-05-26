package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.Map;

public record DouyinAccountStatus(
        String storage,
        String accountKey,
        boolean cookieExists,
        long cookieSizeBytes,
        LocalDateTime cookieUpdatedAt,
        String userId,
        String nickname,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        Integer uploadCooldownMinSeconds,
        Integer uploadCooldownMaxSeconds,
        int todayUploadCount,
        int cooldownWaitingCount,
        boolean enabled,
        Boolean valid,
        String message,
        Map<String, Object> raw
) {
}
