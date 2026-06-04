package com.youbi.monitor.service;

import com.microsoft.playwright.Page;

record DiagnosticArtifactRequest(
        Page page,
        String taskId,
        String runId,
        String platform,
        String source,
        String accountKey,
        int stepIndex,
        String stepName
) {
}
