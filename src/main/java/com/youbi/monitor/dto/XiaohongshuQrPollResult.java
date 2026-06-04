package com.youbi.monitor.dto;

public record XiaohongshuQrPollResult(
        boolean loggedIn,
        String code,
        String message,
        XiaohongshuAccountStatus account
) {
}
