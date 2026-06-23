package com.youbi.monitor.repository;

import com.youbi.monitor.model.DiagnosticArtifactRecord;

import java.util.List;
import java.util.Map;

public interface IDiagnosticArtifactRepositoryService {
    void ensureSchema();

    long countOperatorExecutions(Map<String, String> filters);

    List<Map<String, Object>> listOperatorExecutions(Map<String, String> filters, int offset, int limit);

    long countOperatorDiagnostics(String opId);

    List<DiagnosticArtifactRecord> listOperatorDiagnostics(String opId, int offset, int limit);
}
