package com.youbi.monitor.dto;

public record AliDriveDownloadRequest(
        String remotePath,
        String fileId,
        String outDir,
        String localPath
) {
}
