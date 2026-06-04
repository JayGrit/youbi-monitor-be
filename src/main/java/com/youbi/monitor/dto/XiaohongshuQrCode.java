package com.youbi.monitor.dto;

public record XiaohongshuQrCode(
        String accountKey,
        String authCode,
        String imageDataUrl,
        long expiresAtEpochSeconds
) {
}
