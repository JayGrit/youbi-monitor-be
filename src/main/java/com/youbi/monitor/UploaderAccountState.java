package com.youbi.monitor;

import java.time.LocalDateTime;

public record UploaderAccountState(
        String platform,
        String accountKey,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        Integer uploadCooldownMinSeconds,
        Integer uploadCooldownMaxSeconds,
        int todayUploadCount,
        int cooldownWaitingCount,
        int uploadRunningCount,
        boolean enabled,
        Boolean available,
        String sourceTable,
        LocalDateTime sourceUpdatedAt,
        LocalDateTime metricsUpdatedAt
) {
    public static UploaderAccountState defaults(String platform, String accountKey) {
        return new UploaderAccountState(platform, accountKey, null, null, 3600, 7200,
                0, 0, 0, true, null, null, null, null);
    }
}
