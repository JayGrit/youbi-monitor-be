package com.youbi.monitor;

import java.util.Map;

public record JinritoutiaoUploadResult(
        boolean success,
        String accountKey,
        String accountName,
        String message,
        Map<String, Object> raw
) {
}
