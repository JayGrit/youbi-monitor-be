package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.Map;

public record BilibiliAccountStatus(
        String storage,
        String accountKey,
        boolean cookieExists,
        long cookieSizeBytes,
        LocalDateTime cookieUpdatedAt,
        Long mid,
        String uname,
        String face,
        Integer level,
        Boolean valid,
        String message,
        Map<String, Object> raw
) {
}
