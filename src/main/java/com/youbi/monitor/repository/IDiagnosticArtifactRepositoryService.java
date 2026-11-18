package com.youbi.monitor.repository;

import com.youbi.monitor.model.DiagnosticArtifactRecord;

import java.util.List;
import java.util.Map;

public interface IDiagnosticArtifactRepositoryService {

    List<Map<String, Object>> listOperatorExecutions(Map<String, String> filters);

    List<Map<String, Object>> listOperatorQueue(Map<String, String> filters);

    List<DiagnosticArtifactRecord> listOperatorDiagnostics(String opId);

    DiagnosticArtifactRecord getOperatorDiagnostic(long id);
}
