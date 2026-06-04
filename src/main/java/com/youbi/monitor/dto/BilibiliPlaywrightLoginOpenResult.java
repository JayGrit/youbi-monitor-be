package com.youbi.monitor.dto;

public record BilibiliPlaywrightLoginOpenResult(
        String accountKey,
        String authCode,
        String url,
        long expiresAt
) {
}
