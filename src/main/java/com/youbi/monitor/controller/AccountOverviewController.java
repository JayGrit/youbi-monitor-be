package com.youbi.monitor.controller;

import com.youbi.monitor.dto.AccountDownloaderMaxStagedCountUpdateRequest;
import com.youbi.monitor.dto.AccountNextUploadAllowedAtUpdateRequest;
import com.youbi.monitor.dto.AccountQuietTimeUpdateRequest;
import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.dto.AccountCooldownUpdateRequest;
import com.youbi.monitor.dto.AccountProfileUpdateRequest;
import com.youbi.monitor.dto.SocialAccountKeyUpdateRequest;
import com.youbi.monitor.service.AccountProfileService;
import com.youbi.monitor.service.AccountOverviewService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
public class AccountOverviewController {
    private final AccountOverviewService accountOverviewService;
    private final AccountProfileService accountProfileService;

    public AccountOverviewController(AccountOverviewService accountOverviewService, AccountProfileService accountProfileService) {
        this.accountOverviewService = accountOverviewService;
        this.accountProfileService = accountProfileService;
    }

    // 查询各平台账号总览信息。
    @GetMapping("/api/accounts/overview")
    public Map<String, List<Map<String, Object>>> overview() {
        return accountOverviewService.overview();
    }

    // 查询当前支持的账号平台类型。
    @GetMapping("/api/accounts/types")
    public Map<String, List<String>> types() {
        return Map.of("items", accountOverviewService.types());
    }

    // 查询账号备份服务的最近状态。
    @GetMapping("/api/accounts/backupper-status")
    public BackupperStatus backupperStatus() {
        return accountOverviewService.latestBackupperStatus();
    }

    // 查询指定平台账号的详细信息。
    @GetMapping("/api/accounts/{platform}/{accountKey}")
    public Map<String, Object> account(@PathVariable String platform, @PathVariable String accountKey) {
        return accountOverviewService.account(platform, accountKey);
    }

    // 修改指定平台账号的账号标识。
    @PostMapping("/api/accounts/{platform}/{accountKey}/key")
    public Map<String, Object> renameKey(@PathVariable String platform, @PathVariable String accountKey, @RequestBody SocialAccountKeyUpdateRequest request) {
        return accountOverviewService.renameAccountKey(platform, accountKey, request.newAccountKey());
    }

    // 启用或停用指定平台账号。
    @PostMapping("/api/accounts/{platform}/{accountKey}/enabled")
    public Map<String, Object> setEnabled(@PathVariable String platform, @PathVariable String accountKey, @RequestBody Map<String, Object> request) {
        return accountOverviewService.updateEnabled(platform, accountKey, Boolean.TRUE.equals(request == null ? null : request.get("enabled")));
    }

    // 更新指定平台账号的上传冷却时间。
    @PostMapping("/api/accounts/{platform}/{accountKey}/cooldown")
    public Map<String, Object> setCooldown(@PathVariable String platform, @PathVariable String accountKey, @RequestBody(required = false) AccountCooldownUpdateRequest request) {
        return accountOverviewService.updateCooldown(platform, accountKey, request == null ? null : request.minSeconds(), request == null ? null : request.maxSeconds());
    }

    // 更新指定平台账号的资料信息。
    @PostMapping("/api/accounts/{platform}/{accountKey}/profile")
    public ResponseEntity<?> updateProfile(@PathVariable String platform, @PathVariable String accountKey, @RequestBody AccountProfileUpdateRequest request) {
        try {
            return ResponseEntity.ok(accountProfileService.updateProfile(platform, accountKey, request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 上传指定平台账号的头像文件。
    @PostMapping(value = "/api/accounts/{platform}/{accountKey}/avatar", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadAvatar(@PathVariable String platform, @PathVariable String accountKey, @RequestParam("file") MultipartFile file) {
        try {
            return ResponseEntity.ok(accountProfileService.uploadAvatar(platform, accountKey, file));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 更新指定平台账号的下一次允许上传时间。
    @PostMapping("/api/accounts/{platform}/{accountKey}/next-upload-allowed-at")
    public Map<String, Object> updateNextUploadAllowedAt(
            @PathVariable String platform,
            @PathVariable String accountKey,
            @RequestBody(required = false) AccountNextUploadAllowedAtUpdateRequest request
    ) {
        return accountOverviewService.updateNextUploadAllowedAt(
                platform,
                accountKey,
                request == null ? null : request.nextUploadAllowedAt()
        );
    }

    // 更新指定平台账号的 downloader 最大暂存任务数。
    @PostMapping("/api/accounts/{platform}/{accountKey}/downloader-max-staged-count")
    public Map<String, Object> updateDownloaderMaxStagedCount(
            @PathVariable String platform,
            @PathVariable String accountKey,
            @RequestBody(required = false) AccountDownloaderMaxStagedCountUpdateRequest request
    ) {
        return accountOverviewService.updateDownloaderMaxStagedCount(
                platform,
                accountKey,
                request == null ? null : request.maxStagedCount()
        );
    }

    // 更新指定平台账号的静默时间段。
    @PostMapping("/api/accounts/{platform}/{accountKey}/quiet-time")
    public Map<String, Object> updateQuietTime(
            @PathVariable String platform,
            @PathVariable String accountKey,
            @RequestBody(required = false) AccountQuietTimeUpdateRequest request
    ) {
        return accountOverviewService.updateQuietTime(
                platform,
                accountKey,
                request == null ? null : request.startTime(),
                request == null ? null : request.endTime()
        );
    }
}
