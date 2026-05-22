package com.youbi.monitor;

public record XiaohongshuQrCode(
        String accountKey,
        String authCode,
        String imageDataUrl,
        long expiresAtEpochSeconds
) {
}
