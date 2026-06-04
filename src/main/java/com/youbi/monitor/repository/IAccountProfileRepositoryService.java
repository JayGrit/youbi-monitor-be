package com.youbi.monitor.repository;

import com.youbi.monitor.dto.AccountProfileUpdateResult;

public interface IAccountProfileRepositoryService {
    int updateDisplayName(String table, String accountKey, String displayName);

    int updateAvatarUrl(String table, String accountKey, String avatarUrl);

    AccountProfileUpdateResult findProfile(String table, String accountKey);

    void ensureProfileColumns(String table);
}
