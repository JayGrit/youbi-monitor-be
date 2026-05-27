package com.youbi.monitor;

import java.util.concurrent.atomic.AtomicInteger;

final class DiagnosticRunContext {
    private final String taskId;
    private final String runId;
    private final String platform;
    private final String source;
    private final String accountKey;
    private final AtomicInteger sequence = new AtomicInteger(0);

    DiagnosticRunContext(String taskId, String runId, String platform, String source, String accountKey) {
        this.taskId = TextSupport.firstText(taskId, "manual");
        this.runId = TextSupport.firstText(runId, this.taskId);
        this.platform = TextSupport.text(platform);
        this.source = TextSupport.text(source);
        this.accountKey = TextSupport.text(accountKey);
    }

    int nextStepIndex() {
        return sequence.incrementAndGet();
    }

    String taskId() {
        return taskId;
    }

    String runId() {
        return runId;
    }

    String platform() {
        return platform;
    }

    String source() {
        return source;
    }

    String accountKey() {
        return accountKey;
    }
}
