package com.youbi.monitor;

public record BilibiliPlaywrightLoginPollResult(
        boolean success,
        String code,
        String message,
        BilibiliPlaywrightAccountStatus status
) {
}
