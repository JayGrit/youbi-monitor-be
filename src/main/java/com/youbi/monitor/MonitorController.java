package com.youbi.monitor;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.CONFLICT;

@RestController
@CrossOrigin
public class MonitorController {
    private final MonitorService monitorService;

    public MonitorController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    @GetMapping("/api/video-tasks/monitor")
    public MonitorResponse monitor(@RequestParam(defaultValue = "100") int limit) {
        int boundedLimit = Math.max(1, Math.min(limit, 500));
        return monitorService.listTasks(boundedLimit);
    }

    @PostMapping("/api/video-tasks/{taskId}/ready")
    public java.util.Map<String, String> markReady(@PathVariable String taskId) {
        if (!monitorService.markTaskReady(taskId)) {
            throw new ResponseStatusException(CONFLICT, "Task has no failed status or does not exist.");
        }
        return java.util.Map.of("status", "ready");
    }

    @GetMapping("/health")
    public java.util.Map<String, String> health() {
        return java.util.Map.of("status", "ok");
    }
}
