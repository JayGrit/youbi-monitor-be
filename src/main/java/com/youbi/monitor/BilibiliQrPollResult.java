package com.youbi.monitor;

public record BilibiliQrPollResult(
        boolean loggedIn,
        int code,
        String message,
        BilibiliAccountStatus account
) {
}
