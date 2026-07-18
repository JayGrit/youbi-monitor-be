package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.AliDriveRepository;
import com.youbi.monitor.repository.IAliDriveRepositoryService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AliDriveRepositoryServiceImpl implements IAliDriveRepositoryService {
    private static final String ACCOUNT_TABLE = "uploader_account_alidrive";
    private static final String LEGACY_ACCOUNT_TABLE = "yd_alidrive_account";

    private final AliDriveRepository repository;

    public AliDriveRepositoryServiceImpl(AliDriveRepository repository) {
        this.repository = repository;
    }

    @Override
    public void persistAccountToken(
            String topic,
            String refreshToken,
            String userId,
            String userName,
            String nickName,
            String defaultDriveId
    ) {
        repository.update("""
                INSERT INTO uploader_account_alidrive (
                    topic, refresh_token, user_id, user_name, nick_name, default_drive_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    refresh_token = VALUES(refresh_token),
                    user_id = VALUES(user_id),
                    user_name = VALUES(user_name),
                    nick_name = VALUES(nick_name),
                    default_drive_id = VALUES(default_drive_id),
                    updated_at = CURRENT_TIMESTAMP
                """, topic, refreshToken, userId, userName, nickName, defaultDriveId);
    }

    @Override
    public String loadRefreshToken(String topic) {
        List<String> tokens = repository.query(
                "SELECT refresh_token FROM uploader_account_alidrive WHERE topic = ? LIMIT 1",
                (rs, rowNum) -> rs.getString("refresh_token"),
                topic
        );
        return tokens.isEmpty() ? "" : text(tokens.get(0));
    }

    @Override
    public void ensureAccountSchema() {
    }

    private boolean tableExists(String tableName) {
        return true;
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }
}
