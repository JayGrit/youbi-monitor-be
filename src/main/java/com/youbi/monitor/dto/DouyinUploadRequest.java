package com.youbi.monitor.dto;

public record DouyinUploadRequest(
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
        String coverPath,
        String coverUrl,
        String schedule,
        String productLink,
        String productTitle
) {
}
