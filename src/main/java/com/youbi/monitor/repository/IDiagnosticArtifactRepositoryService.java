package com.youbi.monitor.repository;

import com.youbi.monitor.model.DiagnosticArtifactRecord;

import java.util.List;

public interface IDiagnosticArtifactRepositoryService {
    void ensureSchema();

    List<DiagnosticArtifactRecord> listByTaskId(String taskId);

    List<DiagnosticArtifactRecord> listByTaskIdAndRunId(String taskId, String runId);

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
