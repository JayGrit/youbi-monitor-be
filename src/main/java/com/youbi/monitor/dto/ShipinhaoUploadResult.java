package com.youbi.monitor.dto;

import java.util.Map;

public record ShipinhaoUploadResult(
        boolean success,
        String accountKey,
        String accountName,
        String message,
        Map<String, Object> raw
) {
}
