package com.youbi.monitor.dto;

public record AliDriveUploadRequest(
        String localPath,
        String remoteDir,
        String remotePath,
        String checkNameMode
) {
}
