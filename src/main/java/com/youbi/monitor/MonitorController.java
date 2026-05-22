package com.youbi.monitor;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

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

    @GetMapping("/api/video-tasks/{taskId}/flow")
    public TaskFlowDetail flow(@PathVariable String taskId) {
        TaskFlowDetail detail = monitorService.getTaskFlow(taskId);
        if (detail == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        return detail;
    }

    @PatchMapping("/api/video-tasks/{taskId}/speaker-segments/{segmentId}/dst-text")
    public MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(
            @PathVariable String taskId,
            @PathVariable long segmentId,
            @RequestBody SpeakerSegmentTextUpdateRequest request
    ) {
        MonitorService.SpeakerSegmentTextUpdateResult result = monitorService.updateSpeakerSegmentDstText(
                taskId,
                segmentId,
                request == null ? null : request.dstText()
        );
        if (result == null) {
            throw new ResponseStatusException(NOT_FOUND, "Speaker segment does not exist.");
        }
        return result;
    }

    @PostMapping("/api/video-tasks/{taskId}/ready")
    public java.util.Map<String, String> markReady(@PathVariable String taskId) {
        if (!monitorService.markTaskReady(taskId)) {
            throw new ResponseStatusException(CONFLICT, "Task has no failed status or does not exist.");
        }
        return java.util.Map.of("status", "ready");
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

    @GetMapping("/health")
    public java.util.Map<String, String> health() {
        return java.util.Map.of("status", "ok");
    }
}
