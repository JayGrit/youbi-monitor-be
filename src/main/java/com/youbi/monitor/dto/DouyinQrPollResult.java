package com.youbi.monitor.dto;

public record DouyinQrPollResult(
        boolean loggedIn,
        String code,
        String message,
        DouyinAccountStatus account
) {
}
