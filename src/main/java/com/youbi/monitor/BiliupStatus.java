package com.youbi.monitor;

import java.time.LocalDateTime;

public record BiliupStatus(
        String binPath,
        boolean binExists,
        String cookiePath,
        boolean cookieExists,
        long cookieSizeBytes,
        LocalDateTime cookieUpdatedAt
) {
}
