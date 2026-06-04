package com.youbi.monitor.dto;

public record AccountProfileUpdateRequest(
        String displayName,
        String avatarUrl
) {
}
