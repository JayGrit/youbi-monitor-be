package com.youbi.monitor.repository;

import com.youbi.monitor.dto.JinritoutiaoAccountStatus;
import com.youbi.monitor.model.SocialAccountProfile;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface IJinritoutiaoAccountRepositoryService {
    void ensureSchema();

    List<JinritoutiaoAccountStatus> listAccounts();

    boolean existsTopic(String topic);

    boolean renameTopic(String oldTopic, String newTopic);

    void saveStorageState(String topic, String userId, String nickname, String storageState);

    Optional<String> findStorageState(String topic);

    Optional<LocalDateTime> findUpdatedAt(String topic);

    SocialAccountProfile findProfile(String topic);
}
