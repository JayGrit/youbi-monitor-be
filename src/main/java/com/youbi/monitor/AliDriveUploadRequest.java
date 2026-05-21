package com.youbi.monitor;

public record AliDriveUploadRequest(
        String localPath,
        String remoteDir,
        String remotePath,
        String checkNameMode
) {
}
