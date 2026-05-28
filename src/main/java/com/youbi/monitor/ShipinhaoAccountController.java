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
public class ShipinhaoAccountController {
    private final ShipinhaoAccountService accountService;

    public ShipinhaoAccountController(ShipinhaoAccountService accountService) {
        this.accountService = accountService;
    }

    @GetMapping("/api/shipinhao/accounts")
    public Object accounts() {
        return accountService.accounts();
    }

    @GetMapping("/api/shipinhao/account")
    public ResponseEntity<?> account(@RequestParam(defaultValue = "default") String accountKey) {
        try {
            return ResponseEntity.ok(accountService.status(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/shipinhao/account/{accountKey}/key")
    public ResponseEntity<?> renameKey(@PathVariable String accountKey, @RequestBody SocialAccountKeyUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.renameAccountKey(accountKey, request.newAccountKey()));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/shipinhao/account/{accountKey}/enabled")
    public ResponseEntity<?> setEnabled(@PathVariable String accountKey, @RequestBody Map<String, Object> request) {
        try {
            boolean enabled = Boolean.TRUE.equals(request == null ? null : request.get("enabled"));
            return ResponseEntity.ok(accountService.setEnabled(accountKey, enabled));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/shipinhao/account/{accountKey}/cooldown")
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
}
