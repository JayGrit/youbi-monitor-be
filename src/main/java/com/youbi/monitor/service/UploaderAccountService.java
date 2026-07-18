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

    public Optional<UploaderAccountState> state(String platform, String topic) {
        return repositoryService.state(platform, topic);
    }

    public boolean renameTopic(String platform, String oldTopic, String newTopic) {
        return repositoryService.renameTopic(platform, oldTopic, newTopic);
    }

    public boolean updateEnabled(String platform, String topic, boolean enabled) {
        return repositoryService.updateEnabled(platform, topic, enabled);
    }

    public boolean updateAvailable(String platform, String topic, boolean available) {
        return repositoryService.updateAvailable(platform, topic, available);
    }

    public boolean updateCooldown(String platform, String topic, int minSeconds, int maxSeconds) {
        return repositoryService.updateCooldown(platform, topic, minSeconds, maxSeconds);
    }

    public boolean updateQuietTime(String platform, String topic, LocalTime startTime, LocalTime endTime) {
        return repositoryService.updateQuietTime(platform, topic, startTime, endTime);
    }

    public boolean updateDownloaderMaxStagedCount(String platform, String topic, int maxStagedCount) {
        return repositoryService.updateDownloaderMaxStagedCount(platform, topic, maxStagedCount);
    }
}
