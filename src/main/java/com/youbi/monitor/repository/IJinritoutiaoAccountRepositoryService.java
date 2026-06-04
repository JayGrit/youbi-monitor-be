package com.youbi.monitor.repository;

import com.youbi.monitor.dto.JinritoutiaoAccountStatus;
import com.youbi.monitor.model.SocialAccountProfile;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface IJinritoutiaoAccountRepositoryService {
    void ensureSchema();

    List<JinritoutiaoAccountStatus> listAccounts();

    boolean existsAccountKey(String accountKey);

    boolean renameAccountKey(String oldAccountKey, String newAccountKey);

    void saveStorageState(String accountKey, String userId, String nickname, String storageState);

    Optional<String> findStorageState(String accountKey);

    Optional<LocalDateTime> findUpdatedAt(String accountKey);

    SocialAccountProfile findProfile(String accountKey);
}
