package com.youbi.monitor.controller;

import com.youbi.monitor.dto.MonitorResponse;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.TaskFlowDetail;
import com.youbi.monitor.model.TaskProgressDetail;
import com.youbi.monitor.model.TaskProgressBatchRequest;
import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@CrossOrigin
public class MonitorTaskController {
    private final MonitorService monitorService;

    public MonitorTaskController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    // 分页查询监控页的视频任务列表。
    @GetMapping("/api/video-tasks/monitor")
    public MonitorResponse monitor(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "all") String status,
            @RequestParam(defaultValue = "all") String type,
            @RequestParam(defaultValue = "all") String stage,
            @RequestParam(defaultValue = "") String taskId,
            @RequestParam(defaultValue = "created_desc") String sort
    ) {
        int boundedPage = Math.max(1, page);
        int boundedLimit = Math.min(100, Math.max(1, limit));
        return monitorService.listTasks(boundedPage, boundedLimit, status, type, stage, taskId, sort);
    }

    // 查询各流水线服务的心跳状态。
    @GetMapping("/api/services/heartbeats")
    public List<ServiceHeartbeat> heartbeats() {
        return monitorService.listServiceHeartbeats();
    }

    // 查询指定任务在指定阶段的流转详情。
    @GetMapping("/api/video-tasks/{taskId}/flow")
    public TaskFlowDetail flow(
            @PathVariable String taskId,
            @RequestParam(defaultValue = "downloader") String stage
    ) {
        TaskFlowDetail detail = monitorService.getTaskFlow(taskId, stage);
        if (detail == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        return detail;
    }

    // 查询指定任务的整体进度详情。
    @GetMapping("/api/video-tasks/{taskId}/progress")
    public TaskProgressDetail progress(@PathVariable String taskId) {
        TaskProgressDetail detail = monitorService.getTaskProgress(taskId);
        if (detail == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        return detail;
    }

    // 批量查询多个任务的整体进度详情。
    @PostMapping("/api/video-tasks/progress/batch")
    public List<TaskProgressDetail> progressBatch(@RequestBody TaskProgressBatchRequest request) {
        List<String> taskIds = request == null || request.taskIds() == null
                ? List.of()
                : request.taskIds().stream()
                        .filter(java.util.Objects::nonNull)
                        .map(String::trim)
                        .filter(value -> !value.isBlank())
                        .distinct()
                        .limit(20)
                        .toList();
        return monitorService.getTaskProgressBatch(taskIds);
    }
}
