package com.youbi.monitor.service;

import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.repository.IUploaderAccountRepositoryService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class UploaderAccountService {
    private final IUploaderAccountRepositoryService repositoryService;

    public UploaderAccountService(IUploaderAccountRepositoryService repositoryService) {
        this.repositoryService = repositoryService;
        this.repositoryService.ensureSchema();
    }

    public UploaderAccountState syncFromPlatformRow(
            String platform,
            String accountKey,
            String sourceTable,
            Boolean enabled,
            Integer minSeconds,
            Integer maxSeconds,
            LocalDateTime lastUploadAt,
            LocalDateTime nextUploadAllowedAt,
            LocalDateTime sourceUpdatedAt
    ) {
        return repositoryService.syncFromPlatformRow(
                platform,
                accountKey,
                sourceTable,
                enabled,
                minSeconds,
                maxSeconds,
                lastUploadAt,
                nextUploadAllowedAt,
                sourceUpdatedAt
        );
    }

    public Optional<UploaderAccountState> state(String platform, String accountKey) {
        return repositoryService.state(platform, accountKey);
    }

    public UploaderAccountState updateEnabled(String platform, String accountKey, boolean enabled) {
        return repositoryService.updateEnabled(platform, accountKey, enabled);
    }

    public UploaderAccountState updateAvailable(String platform, String accountKey, boolean available) {
        return repositoryService.updateAvailable(platform, accountKey, available);
    }

    public UploaderAccountState updateCooldown(String platform, String accountKey, Integer minSeconds, Integer maxSeconds) {
        return repositoryService.updateCooldown(platform, accountKey, minSeconds, maxSeconds);
    }

    public void renameAccount(String platform, String oldKey, String newKey) {
        repositoryService.renameAccount(platform, oldKey, newKey);
    }

    public void refreshPlatformMetrics(String platform) {
        repositoryService.refreshPlatformMetrics(platform);
    }
}
