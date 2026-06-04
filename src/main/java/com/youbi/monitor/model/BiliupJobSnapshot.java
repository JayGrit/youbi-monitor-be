package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record BiliupJobSnapshot(
        String id,
        String command,
        String status,
        Integer exitCode,
        LocalDateTime startedAt,
        LocalDateTime completedAt,
        String output
) {
}
