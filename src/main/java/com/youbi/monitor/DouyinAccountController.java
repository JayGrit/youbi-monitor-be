package com.youbi.monitor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class DouyinAccountController {
    private final DouyinAccountService accountService;

    public DouyinAccountController(DouyinAccountService accountService) {
        this.accountService = accountService;
    }

    @GetMapping("/api/douyin/accounts")
    public Object accounts() {
        return accountService.accounts();
    }

    @GetMapping("/api/douyin/cdp-sessions")
    public Object cdpSessions() {
        return accountService.cdpSessions();
    }

    @PostMapping("/api/douyin/cdp-sessions")
    public ResponseEntity<?> saveCdpSession(@RequestBody DouyinCdpSessionUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.saveCdpSession(request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @GetMapping("/api/douyin/account")
    public ResponseEntity<?> account(@RequestParam(defaultValue = "default") String accountKey) {
        try {
            return ResponseEntity.ok(accountService.status(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/douyin/account/qrcode")
    public ResponseEntity<?> qrcode(@RequestParam(defaultValue = "_auto") String accountKey) {
        try {
            return ResponseEntity.ok(accountService.createQrCode(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/douyin/account/{accountKey}/qrcode/{authCode}/poll")
    public ResponseEntity<?> poll(@PathVariable String accountKey, @PathVariable String authCode) {
        try {
            return ResponseEntity.ok(accountService.pollQrCode(accountKey, authCode));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/douyin/account/{accountKey}/key")
    public ResponseEntity<?> renameKey(@PathVariable String accountKey, @RequestBody SocialAccountKeyUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.renameAccountKey(accountKey, request.newAccountKey()));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/douyin/account/{accountKey}/enabled")
    public ResponseEntity<?> setEnabled(@PathVariable String accountKey, @RequestBody Map<String, Object> request) {
        try {
            boolean enabled = Boolean.TRUE.equals(request == null ? null : request.get("enabled"));
            return ResponseEntity.ok(accountService.setEnabled(accountKey, enabled));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
