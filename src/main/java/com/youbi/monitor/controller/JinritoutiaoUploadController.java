package com.youbi.monitor.controller;

import com.youbi.monitor.dto.JinritoutiaoUploadRequest;
import com.youbi.monitor.service.MonitorAsyncUploadService;
import com.youbi.monitor.dto.MonitorUploadTaskResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class JinritoutiaoUploadController {
    private final MonitorAsyncUploadService uploadService;

    public JinritoutiaoUploadController(MonitorAsyncUploadService uploadService) {
        this.uploadService = uploadService;
    }

    @PostMapping("/api/jinritoutiao/upload")
    public ResponseEntity<?> upload(@RequestBody JinritoutiaoUploadRequest request) {
        try {
            MonitorUploadTaskResponse response = uploadService.submit("jinritoutiao", request);
            return response.accepted()
                    ? ResponseEntity.accepted().body(response)
                    : ResponseEntity.status(HttpStatus.CONFLICT).body(response);
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @GetMapping("/api/jinritoutiao/upload/status")
    public ResponseEntity<?> uploadStatus(@RequestParam String uploadTaskId) {
        MonitorUploadTaskResponse response = uploadService.status(uploadTaskId);
        return response.accepted() ? ResponseEntity.ok(response) : ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }
}
