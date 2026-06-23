package com.youbi.monitor.service;

import com.youbi.monitor.model.DiagnosticArtifactRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DiagnosticArtifactService {
    private static final Logger log = LoggerFactory.getLogger(DiagnosticArtifactService.class);

    DiagnosticArtifactRecord archive(DiagnosticArtifactRequest request) {
        String taskId = TextSupport.firstText(request.taskId(), "manual");
        String runId = TextSupport.firstText(request.runId(), taskId);
        String platform = TextSupport.firstText(request.platform(), "unknown");
        String source = TextSupport.firstText(request.source(), "unknown");
        String accountKey = TextSupport.text(request.accountKey());
        String stepName = TextSupport.firstText(request.stepName(), "snapshot");
        int stepIndex = Math.max(1, request.stepIndex());
        log.debug("Monitor diagnostic archive is disabled taskId={} runId={} platform={} source={} stepIndex={} stepName={}",
                taskId, runId, platform, source, stepIndex, stepName);
        return new DiagnosticArtifactRecord(null, taskId, runId, platform, source, emptyToNull(accountKey), null, null, stepIndex, stepName,
                null, null, null, null, null, null, "disabled", null, null);
    }

    private String emptyToNull(String value) {
        String text = TextSupport.text(value);
        return text.isBlank() ? null : text;
    }
}
