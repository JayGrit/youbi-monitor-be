package com.youbi.monitor.dto;

public record BilibiliPlaywrightLoginPollResult(
        boolean success,
        String code,
        String message,
        BilibiliPlaywrightAccountStatus status
) {
}
