package com.youbi.monitor;

public record BilibiliPlaywrightLoginOpenResult(
        String accountKey,
        String authCode,
        String url,
        long expiresAt
) {
}
