package com.youbi.monitor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class BilibiliUploadController {
    private final BilibiliUploadService uploadService;

    public BilibiliUploadController(BilibiliUploadService uploadService) {
        this.uploadService = uploadService;
    }

    @PostMapping("/api/bilibili/upload")
    public ResponseEntity<?> upload(@RequestBody BilibiliUploadRequest request) {
        try {
            BilibiliUploadResult result = uploadService.upload(request);
            if (result.success()) {
                return ResponseEntity.ok(result);
            }
            return ResponseEntity.badRequest().body(result);
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
