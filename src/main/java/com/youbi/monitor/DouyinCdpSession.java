package com.youbi.monitor;

import java.time.LocalDateTime;

public record DouyinCdpSession(
        String accountKey,
        Integer cdpPort,
        String cdpEndpoint,
        String note,
        LocalDateTime lastUploadAt,
        LocalDateTime nextUploadAllowedAt,
        LocalDateTime updatedAt
) {
}
