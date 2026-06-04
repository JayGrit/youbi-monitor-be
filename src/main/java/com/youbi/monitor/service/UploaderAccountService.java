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
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public Optional<UploaderAccountState> state(String platform, String accountKey) {
        return repositoryService.state(platform, accountKey);
    }

    public UploaderAccountState updateEnabled(String platform, String accountKey, boolean enabled) {
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public UploaderAccountState updateAvailable(String platform, String accountKey, boolean available) {
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public UploaderAccountState updateCooldown(String platform, String accountKey, Integer minSeconds, Integer maxSeconds) {
        return state(platform, accountKey).orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
    }

    public void renameAccount(String platform, String oldKey, String newKey) {
    }

    public void refreshPlatformMetrics(String platform) {
    }
}
