package com.youbi.monitor.controller;

import com.youbi.monitor.dto.AccountCooldownUpdateRequest;
import com.youbi.monitor.service.AccountProfileService;
import com.youbi.monitor.dto.AccountProfileUpdateRequest;
import com.youbi.monitor.dto.BilibiliAccountKeyUpdateRequest;
import com.youbi.monitor.service.BilibiliAccountService;
import com.youbi.monitor.dto.BilibiliQrCode;
import com.youbi.monitor.dto.BilibiliQrPollResult;
import org.springframework.http.ResponseEntity;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Map;

@RestController
public class BilibiliAccountController {
    private final BilibiliAccountService accountService;
    private final AccountProfileService accountProfileService;

    public BilibiliAccountController(BilibiliAccountService accountService, AccountProfileService accountProfileService) {
        this.accountService = accountService;
        this.accountProfileService = accountProfileService;
    }

    // 查询 Bilibili 账号登录状态或指定账号状态。
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

    // 查询所有 Bilibili 账号列表。
    @GetMapping("/api/bilibili/accounts")
    public Object accounts() {
        return accountService.accounts();
    }

    // 创建 Bilibili 账号扫码登录二维码。
    @PostMapping("/api/bilibili/account/qrcode")
    public BilibiliQrCode qrcode(@RequestParam(defaultValue = "_auto") String accountKey) throws IOException, InterruptedException {
        return accountService.createQrCode(accountKey);
    }

    // 轮询指定 Bilibili 账号二维码登录结果。
    @PostMapping("/api/bilibili/account/{accountKey}/qrcode/{authCode}/poll")
    public BilibiliQrPollResult poll(@PathVariable String accountKey, @PathVariable String authCode) throws IOException, InterruptedException {
        return accountService.pollQrCode(accountKey, authCode);
    }

    // 轮询默认 Bilibili 账号二维码登录结果。
    @PostMapping("/api/bilibili/account/qrcode/{authCode}/poll")
    public BilibiliQrPollResult pollDefault(@PathVariable String authCode) throws IOException, InterruptedException {
        return accountService.pollQrCode("default", authCode);
    }

    // 续期指定 Bilibili 账号登录态。
    @PostMapping("/api/bilibili/account/renew")
    public ResponseEntity<?> renew(@RequestParam(defaultValue = "default") String accountKey) {
        try {
            return ResponseEntity.ok(accountService.renew(accountKey));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 修改指定 Bilibili 账号的账号标识。
    @PostMapping("/api/bilibili/account/{accountKey}/key")
    public ResponseEntity<?> renameKey(@PathVariable String accountKey, @RequestBody BilibiliAccountKeyUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountService.renameAccountKey(accountKey, request.newAccountKey()));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 启用或停用指定 Bilibili 账号。
    @PostMapping("/api/bilibili/account/{accountKey}/enabled")
    public ResponseEntity<?> setEnabled(@PathVariable String accountKey, @RequestBody Map<String, Object> request) {
        try {
            boolean enabled = Boolean.TRUE.equals(request == null ? null : request.get("enabled"));
            return ResponseEntity.ok(accountService.setEnabled(accountKey, enabled));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 更新指定 Bilibili 账号的上传冷却时间。
    @PostMapping("/api/bilibili/account/{accountKey}/cooldown")
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

    // 更新指定 Bilibili 账号的资料信息。
    @PostMapping("/api/bilibili/account/{accountKey}/profile")
    public ResponseEntity<?> updateProfile(@PathVariable String accountKey, @RequestBody AccountProfileUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountProfileService.updateProfile("bilibili", accountKey, request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 上传指定 Bilibili 账号的头像文件。
    @PostMapping(value = "/api/bilibili/account/{accountKey}/avatar", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadAvatar(@PathVariable String accountKey, @RequestParam("file") MultipartFile file) {
        try {
            return ResponseEntity.ok(accountProfileService.uploadAvatar("bilibili", accountKey, file));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
