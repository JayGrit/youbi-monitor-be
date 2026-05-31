package com.youbi.monitor;

import java.util.Map;

public record KuaishouUploadResult(
        boolean success,
        String accountKey,
        String accountName,
        String message,
        Map<String, Object> raw
) {
}
