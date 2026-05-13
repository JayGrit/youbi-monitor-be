package com.youbi.monitor;

import java.util.Map;

public record BilibiliUploadResult(
        boolean success,
        String bvid,
        Long aid,
        String message,
        Map<String, Object> raw
) {
}
