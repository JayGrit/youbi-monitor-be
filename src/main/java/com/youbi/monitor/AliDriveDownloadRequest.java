package com.youbi.monitor;

public record AliDriveDownloadRequest(
        String remotePath,
        String fileId,
        String outDir,
        String localPath
) {
}
