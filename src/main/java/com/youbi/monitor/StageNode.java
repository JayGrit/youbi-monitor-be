package com.youbi.monitor;

import java.time.LocalDateTime;

public record StageNode(
        String key,
        String label,
        String status,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        long elapsedSeconds,
        String errorMessage
) {
}
