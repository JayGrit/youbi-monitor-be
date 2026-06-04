package com.youbi.monitor.dto;

public record BilibiliQrCode(
        String accountKey,
        String authCode,
        String url,
        long expiresAtEpochSeconds
) {
}
