package com.youbi.monitor.dto;

public record DouyinQrCode(
        String accountKey,
        String authCode,
        String imageDataUrl,
        long expiresAtEpochSeconds
) {
}
