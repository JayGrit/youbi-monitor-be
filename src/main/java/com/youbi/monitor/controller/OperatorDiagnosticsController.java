package com.youbi.monitor.controller;

import com.youbi.monitor.service.OperatorDiagnosticsService;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
// 定义 operator 诊断相关接口的统一路径前缀。
@RequestMapping("/api/operator")
public class OperatorDiagnosticsController {
    private final OperatorDiagnosticsService operatorDiagnosticsService;

    public OperatorDiagnosticsController(OperatorDiagnosticsService operatorDiagnosticsService) {
        this.operatorDiagnosticsService = operatorDiagnosticsService;
    }

    // 查询 operator 任务列表。
    @GetMapping("/tasks")
    public Map<String, Object> tasks(@RequestParam MultiValueMap<String, String> query) {
        return operatorDiagnosticsService.listTasks(query);
    }

    // 查询 operator 队列列表。
    @GetMapping("/queue")
    public Map<String, Object> queue(@RequestParam MultiValueMap<String, String> query) {
        return operatorDiagnosticsService.listQueue(query);
    }

    // 查询指定 operator 任务详情。
    @GetMapping("/tasks/{opId}")
    public Map<String, Object> task(@PathVariable String opId) {
        return operatorDiagnosticsService.getTask(opId);
    }

    // 查询指定 operator 任务的诊断信息。
    @GetMapping("/tasks/{opId}/diagnostics")
    public Map<String, Object> diagnostics(@PathVariable String opId, @RequestParam MultiValueMap<String, String> query) {
        return operatorDiagnosticsService.getDiagnostics(opId, query);
    }
}
