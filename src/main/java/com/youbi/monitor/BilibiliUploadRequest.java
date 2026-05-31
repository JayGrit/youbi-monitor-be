package com.youbi.monitor;

public record BilibiliUploadRequest(
        String accountKey,
        String taskId,
        String videoPath,
        String videoUrl,
        String minioUrl,
        String videoLocation,
        String alidriveFileId,
        String alidriveRemotePath,
        String title,
        String description,
        String tags,
        Integer tid,
        Integer copyright,
        String source,
        String dynamic,
        Integer noReprint,
        String coverPath,
        String coverUrl,
        String line,
        Integer limit
) {
}
