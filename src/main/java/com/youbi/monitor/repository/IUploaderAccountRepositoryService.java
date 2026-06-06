package com.youbi.monitor.repository;

import com.youbi.monitor.dto.UploaderAccountState;

import java.time.LocalTime;
import java.util.Optional;

public interface IUploaderAccountRepositoryService {
    Optional<UploaderAccountState> state(String platform, String accountKey);

    boolean renameAccountKey(String platform, String oldAccountKey, String newAccountKey);

    boolean updateEnabled(String platform, String accountKey, boolean enabled);

    boolean updateAvailable(String platform, String accountKey, boolean available);

    boolean updateCooldown(String platform, String accountKey, int minSeconds, int maxSeconds);

    boolean updateQuietTime(String platform, String accountKey, LocalTime startTime, LocalTime endTime);

    boolean updateDownloaderMaxStagedCount(String platform, String accountKey, int maxStagedCount);

    int resetTodayUploadCounts();
}
