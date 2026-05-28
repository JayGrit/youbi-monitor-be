package com.youbi.monitor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ShipinhaoUploadController {
    private final ShipinhaoUploadService uploadService;

    public ShipinhaoUploadController(ShipinhaoUploadService uploadService) {
        this.uploadService = uploadService;
    }

    @PostMapping("/api/shipinhao/upload")
    public ResponseEntity<?> upload(@RequestBody ShipinhaoUploadRequest request) {
        try {
            ShipinhaoUploadResult result = uploadService.upload(request);
            if (result.success()) {
                return ResponseEntity.ok(result);
            }
            return ResponseEntity.badRequest().body(result);
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
