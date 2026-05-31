package com.youbi.monitor;

public record AccountProfileUpdateRequest(
        String displayName,
        String avatarUrl
) {
}
