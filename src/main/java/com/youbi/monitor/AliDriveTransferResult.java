package com.youbi.monitor;

import java.util.Map;

public record AliDriveTransferResult(
        boolean success,
        String message,
        String fileId,
        String name,
        String remotePath,
        String localPath,
        long size,
        Map<String, Object> raw
) {
}
