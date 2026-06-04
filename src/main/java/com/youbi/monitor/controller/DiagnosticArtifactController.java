package com.youbi.monitor.controller;

import com.youbi.monitor.service.DiagnosticArtifactService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class DiagnosticArtifactController {
    private final DiagnosticArtifactService diagnosticArtifactService;

    public DiagnosticArtifactController(DiagnosticArtifactService diagnosticArtifactService) {
        this.diagnosticArtifactService = diagnosticArtifactService;
    }

    @GetMapping("/api/diagnostics/screenshots")
    public ResponseEntity<?> screenshots(@RequestParam String taskId, @RequestParam(required = false) String runId) {
        try {
            return ResponseEntity.ok(Map.of(
                    "taskId", taskId,
                    "runId", runId == null ? "" : runId.trim(),
                    "items", diagnosticArtifactService.list(taskId, runId)
            ));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
