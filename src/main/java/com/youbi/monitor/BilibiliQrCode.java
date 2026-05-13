package com.youbi.monitor;

public record BilibiliQrCode(
        String accountKey,
        String authCode,
        String url,
        long expiresAtEpochSeconds
) {
}
