package com.youbi.monitor.service;

import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.repository.IUploaderAccountRepositoryService;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.Optional;

@Service
public class UploaderAccountService {
    private final IUploaderAccountRepositoryService repositoryService;

    public UploaderAccountService(IUploaderAccountRepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public Optional<UploaderAccountState> state(String platform, String accountKey) {
        return repositoryService.state(platform, accountKey);
    }

    public boolean renameAccountKey(String platform, String oldAccountKey, String newAccountKey) {
        return repositoryService.renameAccountKey(platform, oldAccountKey, newAccountKey);
    }

    public boolean updateEnabled(String platform, String accountKey, boolean enabled) {
        return repositoryService.updateEnabled(platform, accountKey, enabled);
    }

    public boolean updateCooldown(String platform, String accountKey, int minSeconds, int maxSeconds) {
        return repositoryService.updateCooldown(platform, accountKey, minSeconds, maxSeconds);
    }

    public boolean updateQuietTime(String platform, String accountKey, LocalTime startTime, LocalTime endTime) {
        return repositoryService.updateQuietTime(platform, accountKey, startTime, endTime);
    }

    public boolean updateDownloaderMaxStagedCount(String platform, String accountKey, int maxStagedCount) {
        return repositoryService.updateDownloaderMaxStagedCount(platform, accountKey, maxStagedCount);
    }
}
