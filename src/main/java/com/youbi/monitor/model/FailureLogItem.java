package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record FailureLogItem(
        String id,
        String taskId,
        String title,
        String type,
        String stage,
        String platform,
        String accountKey,
        String errorMessage,
        LocalDateTime failedAt
) {
}
