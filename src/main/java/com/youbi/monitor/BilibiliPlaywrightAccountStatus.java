package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.Map;

public record BilibiliPlaywrightAccountStatus(
        String source,
        String accountKey,
        boolean loggedIn,
        int storageBytes,
        LocalDateTime updatedAt,
        Long mid,
        String uname,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        Boolean valid,
        String message,
        Map<String, Object> raw
) {
}
