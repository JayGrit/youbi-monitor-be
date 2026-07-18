package com.youbi.monitor.dto;

import java.time.LocalDateTime;
import java.util.Map;

public record BackupperStatus(
        String host,
        Map<String, Component> components,
        Map<String, Object> summary,
        LocalDateTime createdAt
) {
    public record Component(
            Long id,
            String host,
            String component,
            String status,
            Map<String, Object> payload,
            String errorMessage,
            LocalDateTime createdAt
    ) {
    }
}
