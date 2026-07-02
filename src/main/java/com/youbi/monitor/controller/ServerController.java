package com.youbi.monitor.controller;

import com.youbi.monitor.dto.BackupperStatus;
import com.youbi.monitor.service.AccountOverviewService;
import com.youbi.monitor.service.BackupperManagementClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ServerController {
    private final AccountOverviewService accountOverviewService;
    private final BackupperManagementClient backupperManagementClient;

    public ServerController(AccountOverviewService accountOverviewService, BackupperManagementClient backupperManagementClient) {
        this.accountOverviewService = accountOverviewService;
        this.backupperManagementClient = backupperManagementClient;
    }

    @GetMapping("/api/server/backupper-status")
    public BackupperStatus backupperStatus() {
        return accountOverviewService.latestBackupperStatus();
    }

    @PostMapping("/api/server/build-cache/clear")
    public Map<String, Object> clearBuildCache() {
        return backupperManagementClient.clearBuildCache();
    }

    @PostMapping("/api/server/diagnostics/clear")
    public Map<String, Object> clearDiagnostics() {
        return backupperManagementClient.clearDiagnostics();
    }
}
