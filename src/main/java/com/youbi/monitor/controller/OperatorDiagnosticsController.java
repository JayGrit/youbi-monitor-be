package com.youbi.monitor.controller;

import com.youbi.monitor.service.OperatorDiagnosticsService;
import com.youbi.monitor.service.OperatorQueueClient;
import org.springframework.util.MultiValueMap;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.Map;

@RestController
// 定义 operator 诊断相关接口的统一路径前缀。
@RequestMapping("/api/operator")
public class OperatorDiagnosticsController {
    private final OperatorDiagnosticsService operatorDiagnosticsService;

    private final OperatorQueueClient operatorQueueClient;

    public OperatorDiagnosticsController(
            OperatorDiagnosticsService operatorDiagnosticsService,
            OperatorQueueClient operatorQueueClient
    ) {
        this.operatorDiagnosticsService = operatorDiagnosticsService;
        this.operatorQueueClient = operatorQueueClient;
    }

    @PostMapping(value = "/queue/{action:delete|priority}", produces = "application/json")
    public ResponseEntity<String> editQueue(@PathVariable String action, @RequestBody String body) {
        return operatorQueueClient.editQueue(action, body);
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
    public Map<String, Object> diagnostics(@PathVariable String opId) {
        return operatorDiagnosticsService.getDiagnostics(opId);
    }

    // 通过后端凭证读取私有 MinIO 诊断附件，避免前端直连私有 bucket。
    @GetMapping("/diagnostics/{id}/{kind}")
    public ResponseEntity<StreamingResponseBody> diagnosticArtifact(@PathVariable long id, @PathVariable String kind) {
        return operatorDiagnosticsService.getDiagnosticArtifact(id, kind);
    }
}
