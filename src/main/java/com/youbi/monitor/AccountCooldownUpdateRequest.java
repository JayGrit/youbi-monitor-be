package com.youbi.monitor;

public record AccountCooldownUpdateRequest(
        Integer minSeconds,
        Integer maxSeconds
) {
}
