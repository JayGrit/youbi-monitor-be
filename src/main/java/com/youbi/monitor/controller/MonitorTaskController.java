package com.youbi.monitor.controller;

import com.youbi.monitor.dto.MonitorResponse;
import com.youbi.monitor.model.DiagnosticArtifactRecord;
import com.youbi.monitor.model.TaskFlowDetail;
import com.youbi.monitor.service.DiagnosticArtifactService;
import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@CrossOrigin
public class MonitorTaskController {
    private final MonitorService monitorService;
    private final DiagnosticArtifactService diagnosticArtifactService;

    public MonitorTaskController(MonitorService monitorService, DiagnosticArtifactService diagnosticArtifactService) {
        this.monitorService = monitorService;
        this.diagnosticArtifactService = diagnosticArtifactService;
    }

    @GetMapping("/api/video-tasks/monitor")
    public MonitorResponse monitor(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int limit
    ) {
        int boundedPage = Math.max(1, page);
        int boundedLimit = Math.min(100, Math.max(1, limit));
        return monitorService.listTasks(boundedPage, boundedLimit);
    }

    @GetMapping("/api/video-tasks/{taskId}/flow")
    public TaskFlowDetail flow(@PathVariable String taskId) {
        TaskFlowDetail detail = monitorService.getTaskFlow(taskId);
        if (detail == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        return detail;
    }

    @GetMapping("/api/video-tasks/{taskId}/uploader-diagnostics")
    public List<DiagnosticArtifactRecord> uploaderDiagnostics(@PathVariable String taskId) {
        return diagnosticArtifactService.list(taskId, null);
    }
}
