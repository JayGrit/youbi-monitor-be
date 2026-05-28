package com.youbi.monitor;

public record ShipinhaoUploadRequest(
        String accountKey,
        String taskId,
        String videoPath,
        String videoUrl,
        String minioUrl,
        String title,
        String description,
        String tags,
        String coverPath,
        String coverUrl,
        String shortTitle,
        Boolean draft
) {
}
