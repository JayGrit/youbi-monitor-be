package com.youbi.monitor.dto;

import java.util.Map;

public record AliDrivePreflightResult(
        boolean success,
        String message,
        boolean downloadable,
        String fileId,
        String name,
        String remotePath,
        long size,
        int httpStatus,
        int probedBytes,
        String downloadHost,
        Map<String, Object> raw
) {
}
