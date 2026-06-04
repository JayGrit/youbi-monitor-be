package com.youbi.monitor.dto;

import java.time.LocalDateTime;
import java.util.Map;

public record ShipinhaoAccountStatus(
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
        int uploadRunningCount,
        boolean enabled,
        Boolean valid,
        String message,
        Map<String, Object> raw,
        String displayName,
        String avatarUrl
) {
    public ShipinhaoAccountStatus(
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
            int uploadRunningCount,
            boolean enabled,
            Boolean valid,
            String message,
            Map<String, Object> raw
    ) {
        this(storage, accountKey, cookieExists, cookieSizeBytes, cookieUpdatedAt, userId, nickname,
                lastUploadAt, nextUploadAllowedAt, uploadCooldownMinSeconds, uploadCooldownMaxSeconds,
                todayUploadCount, cooldownWaitingCount, uploadRunningCount, enabled, valid, message, raw, null, null);
    }
}
