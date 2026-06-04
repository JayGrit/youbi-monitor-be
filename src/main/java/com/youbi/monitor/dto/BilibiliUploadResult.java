package com.youbi.monitor.dto;

import java.util.Map;

public record BilibiliUploadResult(
        boolean success,
        String bvid,
        Long aid,
        Long accountUid,
        String accountName,
        String message,
        Map<String, Object> raw
) {
}
