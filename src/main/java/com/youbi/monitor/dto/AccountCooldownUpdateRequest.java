package com.youbi.monitor.dto;

public record AccountCooldownUpdateRequest(
        Integer minSeconds,
        Integer maxSeconds
) {
}
