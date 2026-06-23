package com.youbi.monitor.repository;

import com.youbi.monitor.model.DiagnosticArtifactRecord;

public interface IDiagnosticArtifactRepositoryService {
    void ensureSchema();

    Long insertUploadedArtifact(
            String taskId,
            String runId,
            String platform,
            String source,
            String accountKey,
            int stepIndex,
            String stepName,
            String screenshotUrl,
            String htmlUrl,
            Long screenshotSizeBytes,
            Long htmlSizeBytes,
            Integer screenshotWidth,
            Integer screenshotHeight
    );
}
