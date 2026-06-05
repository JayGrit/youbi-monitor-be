package com.youbi.monitor.controller;

import com.youbi.monitor.dto.AccountNextUploadAllowedAtUpdateRequest;
import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.service.AccountOverviewService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class AccountOverviewController {
    private final AccountOverviewService accountOverviewService;

    public AccountOverviewController(AccountOverviewService accountOverviewService) {
        this.accountOverviewService = accountOverviewService;
    }

    @GetMapping("/api/accounts/overview")
    public Map<String, List<Map<String, Object>>> overview() {
        return accountOverviewService.overview();
    }

    @GetMapping("/api/accounts/backupper-status")
    public BackupperStatus backupperStatus() {
        return accountOverviewService.latestBackupperStatus();
    }

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
}
