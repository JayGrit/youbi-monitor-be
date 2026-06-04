package com.youbi.monitor.dto;

public record BilibiliQrPollResult(
        boolean loggedIn,
        int code,
        String message,
        BilibiliAccountStatus account
) {
}
