package com.youbi.monitor.repository;

import com.youbi.monitor.model.DiagnosticArtifactRecord;

import java.util.List;
import java.util.Map;

public interface IDiagnosticArtifactRepositoryService {

    List<Map<String, Object>> listOperatorExecutions(Map<String, String> filters);

    List<Map<String, Object>> listOperatorQueue(Map<String, String> filters, Integer limit, long offset);

    QueueStats operatorQueueStats(Map<String, String> filters);

    record QueueStats(long total, boolean hasActive) {}

    List<DiagnosticArtifactRecord> listOperatorDiagnostics(String opId);

    DiagnosticArtifactRecord getOperatorDiagnostic(long id);
}
