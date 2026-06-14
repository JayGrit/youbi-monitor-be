package com.youbi.monitor.model;

public record FailureLogActualPublishedResult(
        String taskId,
        String platform,
        long submissionId,
        String submissionStatus,
        String uploaderStatus,
        String taskStatus
) {
}
