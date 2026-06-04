package com.youbi.monitor.repository;

import com.youbi.monitor.dto.UploaderAccountState;

import java.time.LocalDateTime;
import java.util.Optional;

public interface IUploaderAccountRepositoryService {
    void ensureSchema();

    UploaderAccountState syncFromPlatformRow(
            String platform,
            String accountKey,
            String sourceTable,
            Boolean enabled,
            Integer minSeconds,
            Integer maxSeconds,
            LocalDateTime lastUploadAt,
            LocalDateTime nextUploadAllowedAt,
            LocalDateTime sourceUpdatedAt
    );

    Optional<UploaderAccountState> state(String platform, String accountKey);

    UploaderAccountState updateEnabled(String platform, String accountKey, boolean enabled);

    UploaderAccountState updateAvailable(String platform, String accountKey, boolean available);

    UploaderAccountState updateCooldown(String platform, String accountKey, Integer minSeconds, Integer maxSeconds);

    void renameAccount(String platform, String oldKey, String newKey);

    void refreshPlatformMetrics(String platform);
}
