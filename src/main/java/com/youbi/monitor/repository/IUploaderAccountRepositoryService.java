package com.youbi.monitor.repository;

import com.youbi.monitor.dto.UploaderAccountState;

import java.time.LocalTime;
import java.util.Optional;

public interface IUploaderAccountRepositoryService {
    Optional<UploaderAccountState> state(String platform, String topic);

    boolean renameTopic(String platform, String oldTopic, String newTopic);

    boolean updateEnabled(String platform, String topic, boolean enabled);

    boolean updateAvailable(String platform, String topic, boolean available);

    boolean updateCooldown(String platform, String topic, int minSeconds, int maxSeconds);

    boolean updateQuietTime(String platform, String topic, LocalTime startTime, LocalTime endTime);

    boolean updateDownloaderMaxStagedCount(String platform, String topic, int maxStagedCount);
}
