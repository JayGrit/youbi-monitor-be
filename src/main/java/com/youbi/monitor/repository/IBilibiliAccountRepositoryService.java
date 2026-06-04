package com.youbi.monitor.repository;

import com.youbi.monitor.dto.BilibiliAccountStatus;
import com.youbi.monitor.model.BilibiliAccountProfile;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface IBilibiliAccountRepositoryService {
    void ensureSchema();

    List<BilibiliAccountStatus> listAccounts();

    boolean existsAccountKey(String accountKey);

    boolean renameAccountKey(String oldAccountKey, String newAccountKey);

    Optional<String> findLatestAccountKeyByMid(long mid);

    Optional<String> findLoginInfoJson(String accountKey);

    Optional<LocalDateTime> findUpdatedAt(String accountKey);

    void saveLoginInfo(String accountKey, Long mid, String uname, String loginInfoJson);
}
