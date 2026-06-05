package com.youbi.monitor.dto;

import java.time.LocalDateTime;

public record UploaderAccountState(
        String platform,
        String accountKey,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        Integer uploadCooldownMinSeconds,
        Integer uploadCooldownMaxSeconds,
        int downloaderMaxStagedCount,
        int todayUploadCount,
        int cooldownWaitingCount,
        String uploadRunningTaskId,
        int uploadRunningCount,
        int failedUploadCount,
        boolean enabled,
        Boolean available,
        String sourceTable,
        LocalDateTime sourceUpdatedAt,
        LocalDateTime metricsUpdatedAt
) {
    public static UploaderAccountState defaults(String platform, String accountKey) {
        return new UploaderAccountState(platform, accountKey, null, null, 3600, 7200,
                5, 0, 0, null, 0, 0, true, null, null, null, null);
    }
}
