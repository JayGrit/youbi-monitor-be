package com.youbi.monitor.repository;

import com.youbi.monitor.dto.UploaderAccountState;

import java.util.Optional;

public interface IUploaderAccountRepositoryService {
    Optional<UploaderAccountState> state(String platform, String accountKey);

    boolean renameAccountKey(String platform, String oldAccountKey, String newAccountKey);

    boolean updateEnabled(String platform, String accountKey, boolean enabled);

    boolean updateCooldown(String platform, String accountKey, int minSeconds, int maxSeconds);

    boolean updateDownloaderMaxStagedCount(String platform, String accountKey, int maxStagedCount);

    int resetTodayUploadCounts();
}
