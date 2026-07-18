package com.youbi.monitor.repository;

import com.youbi.monitor.dto.AccountProfileUpdateResult;

public interface IAccountProfileRepositoryService {
    int updateDisplayName(String table, String topic, String displayName);

    int updateAvatarUrl(String table, String topic, String avatarUrl);

    AccountProfileUpdateResult findProfile(String table, String topic);

    void ensureProfileColumns(String table);
}
