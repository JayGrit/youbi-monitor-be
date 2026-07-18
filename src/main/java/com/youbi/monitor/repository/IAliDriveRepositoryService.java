package com.youbi.monitor.repository;

public interface IAliDriveRepositoryService {
    void persistAccountToken(
            String topic,
            String refreshToken,
            String userId,
            String userName,
            String nickName,
            String defaultDriveId
    );

    String loadRefreshToken(String topic);

    void ensureAccountSchema();
}
