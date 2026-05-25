package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.Map;

public record XiaohongshuAccountStatus(
        String storage,
        String accountKey,
        boolean cookieExists,
        long cookieSizeBytes,
        LocalDateTime cookieUpdatedAt,
        String userId,
        String nickname,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        int todayUploadCount,
        int cooldownWaitingCount,
        Boolean valid,
        String message,
        Map<String, Object> raw
) {
}
