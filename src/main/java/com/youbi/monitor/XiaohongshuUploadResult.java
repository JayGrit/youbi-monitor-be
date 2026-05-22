package com.youbi.monitor;

import java.util.Map;

public record XiaohongshuUploadResult(
        boolean success,
        String accountKey,
        String accountName,
        String message,
        Map<String, Object> raw
) {
}
