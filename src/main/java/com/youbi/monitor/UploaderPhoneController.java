package com.youbi.monitor;

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

    @GetMapping("/api/uploader-phones")
    public Object matrix() {
        return uploaderPhoneService.matrix();
    }

    @PostMapping("/api/uploader-phones/{phoneId}")
    public ResponseEntity<?> updatePhone(@PathVariable long phoneId, @RequestBody UploaderPhoneUpdateRequest request) {
        try {
            return ResponseEntity.ok(uploaderPhoneService.updatePhone(phoneId, request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

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
