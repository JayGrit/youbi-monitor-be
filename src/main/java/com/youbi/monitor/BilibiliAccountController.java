package com.youbi.monitor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

@RestController
public class BilibiliAccountController {
    private final BilibiliAccountService accountService;

    public BilibiliAccountController(BilibiliAccountService accountService) {
        this.accountService = accountService;
    }

    @GetMapping("/api/bilibili/account")
    public Object account(@RequestParam(defaultValue = "") String accountKey) throws IOException {
        if (accountKey == null || accountKey.isBlank()) {
            return Map.of(
                    "selected", accountService.status(),
                    "accounts", accountService.accounts()
            );
        }
        return accountService.status(accountKey);
    }

    @GetMapping("/api/bilibili/accounts")
    public Object accounts() {
        return accountService.accounts();
    }

    @PostMapping("/api/bilibili/account/qrcode")
    public BilibiliQrCode qrcode(@RequestParam(defaultValue = "_auto") String accountKey) throws IOException, InterruptedException {
        return accountService.createQrCode(accountKey);
    }

    @PostMapping("/api/bilibili/account/{accountKey}/qrcode/{authCode}/poll")
    public BilibiliQrPollResult poll(@PathVariable String accountKey, @PathVariable String authCode) throws IOException, InterruptedException {
        return accountService.pollQrCode(accountKey, authCode);
    }

    @PostMapping("/api/bilibili/account/qrcode/{authCode}/poll")
    public BilibiliQrPollResult pollDefault(@PathVariable String authCode) throws IOException, InterruptedException {
        return accountService.pollQrCode("default", authCode);
    }

    @PostMapping("/api/bilibili/account/renew")
    public ResponseEntity<?> renew(@RequestParam(defaultValue = "default") String accountKey) {
        try {
            return ResponseEntity.ok(accountService.renew(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/bilibili/account/{accountKey}/key")
    public ResponseEntity<?> renameKey(@PathVariable String accountKey, @RequestBody BilibiliAccountKeyUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.renameAccountKey(accountKey, request.newAccountKey()));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
