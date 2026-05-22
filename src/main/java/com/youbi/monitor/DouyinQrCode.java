package com.youbi.monitor;

public record DouyinQrCode(
        String accountKey,
        String authCode,
        String imageDataUrl,
        long expiresAtEpochSeconds
) {
}
