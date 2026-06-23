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
@RequestMapping("/api/operator")
public class OperatorDiagnosticsController {
    private final OperatorDiagnosticsService operatorDiagnosticsService;

    public OperatorDiagnosticsController(OperatorDiagnosticsService operatorDiagnosticsService) {
        this.operatorDiagnosticsService = operatorDiagnosticsService;
    }

    @GetMapping("/tasks")
    public Map<String, Object> tasks(@RequestParam MultiValueMap<String, String> query) {
        return operatorDiagnosticsService.listTasks(query);
    }

    @GetMapping("/tasks/{opId}")
    public Map<String, Object> task(@PathVariable String opId) {
        return operatorDiagnosticsService.getTask(opId);
    }

    @GetMapping("/tasks/{opId}/diagnostics")
    public Map<String, Object> diagnostics(@PathVariable String opId) {
        return operatorDiagnosticsService.getDiagnostics(opId);
    }
}
