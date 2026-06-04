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
            String accountKey,
            String refreshToken,
            String userId,
            String userName,
            String nickName,
            String defaultDriveId
    ) {
        repository.update("""
                INSERT INTO uploader_account_alidrive (
                    account_key, refresh_token, user_id, user_name, nick_name, default_drive_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    refresh_token = VALUES(refresh_token),
                    user_id = VALUES(user_id),
                    user_name = VALUES(user_name),
                    nick_name = VALUES(nick_name),
                    default_drive_id = VALUES(default_drive_id),
                    updated_at = CURRENT_TIMESTAMP
                """, accountKey, refreshToken, userId, userName, nickName, defaultDriveId);
    }

    @Override
    public String loadRefreshToken(String accountKey) {
        List<String> tokens = repository.query(
                "SELECT refresh_token FROM uploader_account_alidrive WHERE account_key = ? LIMIT 1",
                (rs, rowNum) -> rs.getString("refresh_token"),
                accountKey
        );
        return tokens.isEmpty() ? "" : text(tokens.get(0));
    }

    @Override
    public void ensureAccountSchema() {
        if (!tableExists(ACCOUNT_TABLE) && tableExists(LEGACY_ACCOUNT_TABLE)) {
            repository.execute("RENAME TABLE yd_alidrive_account TO uploader_account_alidrive");
        }
        repository.execute("""
                CREATE TABLE IF NOT EXISTS uploader_account_alidrive (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    refresh_token TEXT NOT NULL,
                    user_id VARCHAR(128) NULL,
                    user_name VARCHAR(128) NULL,
                    nick_name VARCHAR(255) NULL,
                    default_drive_id VARCHAR(128) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);
    }

    private boolean tableExists(String tableName) {
        Integer count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                    AND table_name = ?
                """,
                Integer.class,
                tableName
        );
        return count != null && count > 0;
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }
}
