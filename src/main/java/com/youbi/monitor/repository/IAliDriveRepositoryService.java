package com.youbi.monitor.repository;

public interface IAliDriveRepositoryService {
    void persistAccountToken(
            String accountKey,
            String refreshToken,
            String userId,
            String userName,
            String nickName,
            String defaultDriveId
    );

    String loadRefreshToken(String accountKey);

    void ensureAccountSchema();
}
