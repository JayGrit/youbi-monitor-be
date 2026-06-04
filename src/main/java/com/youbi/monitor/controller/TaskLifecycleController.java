package com.youbi.monitor.controller;

import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.Map;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@CrossOrigin
public class TaskLifecycleController {
    private final MonitorService monitorService;

    public TaskLifecycleController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    @PostMapping("/api/video-tasks/{taskId}/ready")
    public Map<String, String> markReady(@PathVariable String taskId) {
        if (!monitorService.markTaskReady(taskId)) {
            throw new ResponseStatusException(CONFLICT, "Task has no failed status or does not exist.");
        }
        return Map.of("status", "ready");
    }

    @PostMapping("/api/video-tasks/{taskId}/restart")
    public MonitorService.TaskRestartResult restart(@PathVariable String taskId) {
        try {
            MonitorService.TaskRestartResult result = monitorService.restartTask(taskId);
            if (result == null) {
                throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
            }
            return result;
        } catch (IllegalStateException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        } catch (IOException exc) {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/video-tasks/{taskId}/stop")
    public MonitorService.TaskStopResult stop(@PathVariable String taskId) {
        MonitorService.TaskStopResult result = monitorService.stopTask(taskId);
        if (result == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        if (!result.stoppedTask()) {
            throw new ResponseStatusException(CONFLICT, "Task is not running.");
        }
        return result;
    }

    @DeleteMapping("/api/video-tasks/{taskId}")
    public MonitorService.TaskDeleteResult delete(@PathVariable String taskId) {
        try {
            MonitorService.TaskDeleteResult result = monitorService.deleteTask(taskId);
            if (result == null) {
                throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
            }
            return result;
        } catch (IllegalStateException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        } catch (IOException exc) {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, exc.getMessage(), exc);
        }
    }
}
