package com.youbi.monitor;

public record BilibiliUploadRequest(
        String accountKey,
        String taskId,
        String videoPath,
        String title,
        String description,
        String tags,
        Integer tid,
        Integer copyright,
        String source,
        String dynamic,
        Integer noReprint,
        String line,
        Integer limit
) {
}
