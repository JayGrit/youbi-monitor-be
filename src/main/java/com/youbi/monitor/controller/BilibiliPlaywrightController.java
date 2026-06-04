package com.youbi.monitor.controller;

import com.youbi.monitor.service.BilibiliPlaywrightAccountService;
import com.youbi.monitor.service.BilibiliPlaywrightUploadService;
import com.youbi.monitor.dto.BilibiliUploadRequest;
import com.youbi.monitor.dto.BilibiliUploadResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class BilibiliPlaywrightController {
    private final BilibiliPlaywrightAccountService accountService;
    private final BilibiliPlaywrightUploadService uploadService;

    public BilibiliPlaywrightController(BilibiliPlaywrightAccountService accountService, BilibiliPlaywrightUploadService uploadService) {
        this.accountService = accountService;
        this.uploadService = uploadService;
    }

    @GetMapping("/api/bilibili/playwright/accounts")
    public ResponseEntity<?> accounts() {
        return ResponseEntity.ok(accountService.accounts());
    }

    @GetMapping("/api/bilibili/playwright/status")
    public ResponseEntity<?> status(@RequestParam(defaultValue = BilibiliPlaywrightAccountService.DEFAULT_ACCOUNT_KEY) String accountKey) {
        try {
            return ResponseEntity.ok(accountService.status(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/bilibili/playwright/login/open")
    public ResponseEntity<?> openLogin(@RequestBody(required = false) Map<String, String> body) {
        try {
            String accountKey = body == null ? BilibiliPlaywrightAccountService.DEFAULT_ACCOUNT_KEY : body.getOrDefault("accountKey", BilibiliPlaywrightAccountService.DEFAULT_ACCOUNT_KEY);
            return ResponseEntity.ok(accountService.openManualLogin(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/bilibili/playwright/login/poll")
    public ResponseEntity<?> pollLogin(@RequestBody Map<String, String> body) {
        try {
            return ResponseEntity.ok(accountService.pollManualLogin(body.getOrDefault("accountKey", BilibiliPlaywrightAccountService.DEFAULT_ACCOUNT_KEY), body.get("authCode")));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/bilibili/playwright/upload")
    public ResponseEntity<?> upload(@RequestBody BilibiliUploadRequest request) {
        try {
            BilibiliUploadResult result = uploadService.upload(request);
            return ResponseEntity.ok(result);
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/bilibili/playwright/inspect-upload-page")
    public ResponseEntity<?> inspectUploadPage(@RequestBody(required = false) Map<String, String> body) {
        try {
            String accountKey = body == null ? BilibiliPlaywrightAccountService.DEFAULT_ACCOUNT_KEY : body.getOrDefault("accountKey", BilibiliPlaywrightAccountService.DEFAULT_ACCOUNT_KEY);
            return ResponseEntity.ok(uploadService.inspectUploadPage(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/bilibili/playwright/inspect-upload-selection")
    public ResponseEntity<?> inspectUploadSelection(@RequestBody BilibiliUploadRequest request) {
        try {
            return ResponseEntity.ok(uploadService.inspectUploadSelection(request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
