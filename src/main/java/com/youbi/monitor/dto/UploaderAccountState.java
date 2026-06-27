package com.youbi.monitor.dto;

import java.time.LocalDateTime;
import java.time.LocalTime;

public record UploaderAccountState(
        String platform,
        String accountKey,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        Integer uploadCooldownMinSeconds,
        Integer uploadCooldownMaxSeconds,
        LocalTime uploadQuietStartTime,
        LocalTime uploadQuietEndTime,
        int downloaderMaxStagedCount,
        int downloaderPendingCount,
        int stagedRunningCount,
        int stagedFailedCount,
        int todayUploadCount,
        int cooldownWaitingCount,
        String uploadRunningTaskId,
        int uploadRunningCount,
        int failedUploadCount,
        boolean enabled,
        Boolean available,
        String sourceTable,
        LocalDateTime sourceUpdatedAt
) {
    public static UploaderAccountState defaults(String platform, String accountKey) {
        return new UploaderAccountState(platform, accountKey, null, null, 3600, 7200,
                LocalTime.of(1, 0), LocalTime.of(7, 0), 5, 0, 0, 0, 0, 0, null, 0, 0, true, null, null, null);
    }
}
