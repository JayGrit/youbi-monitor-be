package com.youbi.monitor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class XiaohongshuUploadController {
    private final XiaohongshuUploadService uploadService;

    public XiaohongshuUploadController(XiaohongshuUploadService uploadService) {
        this.uploadService = uploadService;
    }

    @PostMapping("/api/xiaohongshu/upload")
    public ResponseEntity<?> upload(@RequestBody XiaohongshuUploadRequest request) {
        try {
            XiaohongshuUploadResult result = uploadService.upload(request);
            if (result.success()) {
                return ResponseEntity.ok(result);
            }
            return ResponseEntity.badRequest().body(result);
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
