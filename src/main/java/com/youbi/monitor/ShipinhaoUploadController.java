package com.youbi.monitor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ShipinhaoUploadController {
    private final MonitorAsyncUploadService uploadService;

    public ShipinhaoUploadController(MonitorAsyncUploadService uploadService) {
        this.uploadService = uploadService;
    }

    @PostMapping("/api/shipinhao/upload")
    public ResponseEntity<?> upload(@RequestBody ShipinhaoUploadRequest request) {
        try {
            MonitorUploadTaskResponse response = uploadService.submit("shipinhao", request);
            return response.accepted()
                    ? ResponseEntity.accepted().body(response)
                    : ResponseEntity.status(HttpStatus.CONFLICT).body(response);
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @GetMapping("/api/shipinhao/upload/status")
    public ResponseEntity<?> uploadStatus(@RequestParam String uploadTaskId) {
        MonitorUploadTaskResponse response = uploadService.status(uploadTaskId);
        return response.accepted() ? ResponseEntity.ok(response) : ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }
}
