package com.youbi.monitor;

public record DouyinQrPollResult(
        boolean loggedIn,
        String code,
        String message,
        DouyinAccountStatus account
) {
}
