package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.Map;

public record BilibiliAccountStatus(
        String storage,
        String accountKey,
        boolean cookieExists,
        long cookieSizeBytes,
        LocalDateTime cookieUpdatedAt,
        Long mid,
        String uname,
        String face,
        Integer level,
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
    public BilibiliAccountStatus(
            String storage,
            String accountKey,
            boolean cookieExists,
            long cookieSizeBytes,
            LocalDateTime cookieUpdatedAt,
            Long mid,
            String uname,
            String face,
            Integer level,
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
        this(storage, accountKey, cookieExists, cookieSizeBytes, cookieUpdatedAt, mid, uname, face, level,
                lastUploadAt, nextUploadAllowedAt, uploadCooldownMinSeconds, uploadCooldownMaxSeconds,
                todayUploadCount, cooldownWaitingCount, uploadRunningCount, enabled, valid, message, raw, null, null);
    }
}
