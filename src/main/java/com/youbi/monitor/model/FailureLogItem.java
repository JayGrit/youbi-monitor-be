package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record FailureLogItem(
        String id,
        String taskId,
        String title,
        String topic,
        String stage,
        String platform,
        String errorMessage,
        LocalDateTime failedAt
) {
}
