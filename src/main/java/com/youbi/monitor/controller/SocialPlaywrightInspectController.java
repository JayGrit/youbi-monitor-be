package com.youbi.monitor.controller;

import com.youbi.monitor.service.SocialPlaywrightInspectService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class SocialPlaywrightInspectController {
    private final SocialPlaywrightInspectService inspectService;

    public SocialPlaywrightInspectController(SocialPlaywrightInspectService inspectService) {
        this.inspectService = inspectService;
    }

    @GetMapping("/api/social/playwright/{platform}/fingerprint")
    public ResponseEntity<?> fingerprint(@PathVariable String platform) {
        try {
            return ResponseEntity.ok(inspectService.fingerprint(platform));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @GetMapping("/api/social/playwright/fingerprints")
    public ResponseEntity<?> fingerprints() {
        return ResponseEntity.ok(inspectService.fingerprintAll());
    }

    @PostMapping("/api/social/playwright/{platform}/inspect-upload-page")
    public ResponseEntity<?> inspectUploadPage(@PathVariable String platform, @RequestBody(required = false) Map<String, String> body) {
        try {
            String accountKey = body == null ? "" : body.get("accountKey");
            return ResponseEntity.ok(inspectService.inspectUploadPage(platform, accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/social/playwright/inspect-upload-pages")
    public ResponseEntity<?> inspectUploadPages(@RequestBody(required = false) Map<String, String> body) {
        return ResponseEntity.ok(inspectService.inspectAllUploadPages(body));
    }
}
