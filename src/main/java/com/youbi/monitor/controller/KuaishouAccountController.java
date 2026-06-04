package com.youbi.monitor.controller;

import com.youbi.monitor.dto.AccountCooldownUpdateRequest;
import com.youbi.monitor.service.AccountProfileService;
import com.youbi.monitor.dto.AccountProfileUpdateRequest;
import com.youbi.monitor.service.KuaishouAccountService;
import com.youbi.monitor.dto.SocialAccountKeyUpdateRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@RestController
public class KuaishouAccountController {
    private final KuaishouAccountService accountService;
    private final AccountProfileService accountProfileService;

    public KuaishouAccountController(KuaishouAccountService accountService, AccountProfileService accountProfileService) {
        this.accountService = accountService;
        this.accountProfileService = accountProfileService;
    }

    @GetMapping("/api/kuaishou/accounts")
    public Object accounts() {
        return accountService.accounts();
    }

    @GetMapping("/api/kuaishou/account")
    public ResponseEntity<?> account(@RequestParam(defaultValue = "default") String accountKey) {
        try {
            return ResponseEntity.ok(accountService.status(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/kuaishou/account/{accountKey}/key")
    public ResponseEntity<?> renameKey(@PathVariable String accountKey, @RequestBody SocialAccountKeyUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.renameAccountKey(accountKey, request.newAccountKey()));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/kuaishou/account/{accountKey}/enabled")
    public ResponseEntity<?> setEnabled(@PathVariable String accountKey, @RequestBody Map<String, Object> request) {
        try {
            boolean enabled = Boolean.TRUE.equals(request == null ? null : request.get("enabled"));
            return ResponseEntity.ok(accountService.setEnabled(accountKey, enabled));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/kuaishou/account/{accountKey}/cooldown")
    public ResponseEntity<?> setCooldown(@PathVariable String accountKey, @RequestBody AccountCooldownUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.setCooldown(
                    accountKey,
                    request == null ? null : request.minSeconds(),
                    request == null ? null : request.maxSeconds()
            ));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/kuaishou/account/{accountKey}/profile")
    public ResponseEntity<?> updateProfile(@PathVariable String accountKey, @RequestBody AccountProfileUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountProfileService.updateProfile("kuaishou", accountKey, request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping(value = "/api/kuaishou/account/{accountKey}/avatar", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadAvatar(@PathVariable String accountKey, @RequestParam("file") MultipartFile file) {
        try {
            return ResponseEntity.ok(accountProfileService.uploadAvatar("kuaishou", accountKey, file));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
