package com.youbi.monitor;

public record XiaohongshuQrPollResult(
        boolean loggedIn,
        String code,
        String message,
        XiaohongshuAccountStatus account
) {
}
