package com.youbi.monitor.controller;

import com.youbi.monitor.service.OperatorQueryClient;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/operator")
public class OperatorDiagnosticsController {
    private final OperatorQueryClient operatorQueryClient;

    public OperatorDiagnosticsController(OperatorQueryClient operatorQueryClient) {
        this.operatorQueryClient = operatorQueryClient;
    }

    @GetMapping("/tasks")
    public ResponseEntity<String> tasks(@RequestParam MultiValueMap<String, String> query) {
        return operatorQueryClient.listTasks(query);
    }

    @GetMapping("/tasks/{opId}")
    public ResponseEntity<String> task(@PathVariable String opId) {
        return operatorQueryClient.getTask(opId);
    }

    @GetMapping("/tasks/{opId}/diagnostics")
    public ResponseEntity<String> diagnostics(@PathVariable String opId) {
        return operatorQueryClient.getTaskDiagnostics(opId);
    }
}
