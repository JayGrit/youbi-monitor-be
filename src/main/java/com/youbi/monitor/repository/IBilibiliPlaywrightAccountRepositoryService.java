package com.youbi.monitor.repository;

import com.youbi.monitor.dto.BilibiliPlaywrightAccountStatus;
import com.youbi.monitor.model.BilibiliAccountProfile;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface IBilibiliPlaywrightAccountRepositoryService {
    void ensureSchema();

    List<BilibiliPlaywrightAccountStatus> listAccounts(AccountAvailabilityResolver availabilityResolver, AccountEnabledResolver enabledResolver);

    Optional<String> findStorageState(String accountKey);

    BilibiliAccountProfile findProfile(String accountKey);

    Optional<LocalDateTime> findUpdatedAt(String accountKey);

    boolean updateStorageState(String accountKey, Long mid, String uname, String storageState);

    @FunctionalInterface
    interface AccountAvailabilityResolver {
        AccountAvailability resolve(String accountKey);
    }

    @FunctionalInterface
    interface AccountEnabledResolver {
        boolean enabled(String accountKey);
    }

    record AccountAvailability(
            LocalDateTime lastUploadAt,
            LocalDateTime nextUploadAllowedAt,
            int todayUploadCount,
            int cooldownWaitingCount,
            int uploadRunningCount
    ) {
    }
}
