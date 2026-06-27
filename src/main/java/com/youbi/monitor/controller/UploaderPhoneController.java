package com.youbi.monitor.controller;

import com.youbi.monitor.dto.UploaderPhoneAccountUpdateRequest;
import com.youbi.monitor.service.UploaderPhoneService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class UploaderPhoneController {
    private final UploaderPhoneService uploaderPhoneService;

    public UploaderPhoneController(UploaderPhoneService uploaderPhoneService) {
        this.uploaderPhoneService = uploaderPhoneService;
    }

    // 查询上传手机与平台账号的绑定矩阵。
    @GetMapping("/api/uploader-phones")
    public Object matrix() {
        return uploaderPhoneService.matrix();
    }

    // 更新指定上传手机在指定平台的账号绑定。
    @PostMapping("/api/uploader-phones/{phoneId}/platform/{platform}")
    public ResponseEntity<?> updatePlatformAccount(
            @PathVariable long phoneId,
            @PathVariable String platform,
            @RequestBody UploaderPhoneAccountUpdateRequest request
    ) {
        try {
            return ResponseEntity.ok(uploaderPhoneService.updatePlatformAccount(phoneId, platform, request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
