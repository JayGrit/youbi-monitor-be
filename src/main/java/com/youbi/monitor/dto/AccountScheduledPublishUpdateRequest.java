package com.youbi.monitor.dto;

public record AccountScheduledPublishUpdateRequest(
        Boolean supported,
        Integer maxDays
) {
}
