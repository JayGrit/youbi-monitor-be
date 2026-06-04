package com.youbi.monitor.dto;

import java.time.LocalDateTime;

public record AccountNextUploadAllowedAtUpdateRequest(
        LocalDateTime nextUploadAllowedAt
) {
}
