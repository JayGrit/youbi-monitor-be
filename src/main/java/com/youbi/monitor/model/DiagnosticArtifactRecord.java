package com.youbi.monitor.model;

import java.time.LocalDateTime;

public record DiagnosticArtifactRecord(
        Long id,
        String taskId,
        String runId,
        String platform,
        String source,
        String topic,
        String publisherJobName,
        String aspectRatio,
        int stepIndex,
        String stepName,
        String screenshotUrl,
        String htmlUrl,
        Long screenshotSizeBytes,
        Long htmlSizeBytes,
        Integer screenshotWidth,
        Integer screenshotHeight,
        String status,
        String errorMessage,
        LocalDateTime createdAt
) {
}
