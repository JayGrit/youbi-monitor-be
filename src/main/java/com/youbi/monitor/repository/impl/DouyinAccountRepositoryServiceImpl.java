package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.DouyinAccountStatus;
import com.youbi.monitor.model.SocialAccountProfile;
import com.youbi.monitor.repository.IDouyinAccountRepositoryService;
import com.youbi.monitor.repository.DouyinAccountRepository;
import com.youbi.monitor.repository.RepositorySchemaSupport;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class DouyinAccountRepositoryServiceImpl implements IDouyinAccountRepositoryService {
    private static final String TABLE = "uploader_account_douyin";

    private final DouyinAccountRepository repository;

    public DouyinAccountRepositoryServiceImpl(DouyinAccountRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
        repository.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_account_douyin (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    account_key VARCHAR(64) NOT NULL,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_account_douyin_account_key (account_key)
                )
                """
        );
        RepositorySchemaSupport.ensureSurrogatePrimaryKey(repository, TABLE);
        RepositorySchemaSupport.ensureColumn(repository, TABLE, "display_name", "VARCHAR(128) NULL");
        RepositorySchemaSupport.ensureColumn(repository, TABLE, "avatar_url", "VARCHAR(1024) NULL");
    }

    @Override
    public List<DouyinAccountStatus> listAccounts() {
        return repository.query(
                """
                SELECT ua.account_key, ua.last_upload_at, ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds, ua.upload_cooldown_max_seconds,
                       ua.today_upload_count, ua.cooldown_waiting_count, ua.upload_running_count,
                       ua.is_enabled,
                       pa.user_id, pa.nickname, pa.storage_state_json, pa.updated_at, pa.display_name, pa.avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_douyin pa ON pa.account_key = ua.account_key
                WHERE ua.platform = 'douyin'
                ORDER BY ua.account_key
                """,
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new DouyinAccountStatus(
                            "database",
                            accountKey,
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            updatedAt,
                            rs.getString("user_id"),
                            rs.getString("nickname"),
                            toLocalDateTime(rs.getTimestamp("last_upload_at")),
                            toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                            nullableInt(rs, "upload_cooldown_min_seconds"),
                            nullableInt(rs, "upload_cooldown_max_seconds"),
                            rs.getInt("today_upload_count"),
                            rs.getInt("cooldown_waiting_count"),
                            rs.getInt("upload_running_count"),
                            rs.getBoolean("is_enabled"),
                            null,
                            json != null && !json.isBlank() ? "已保存" : "未登录",
                            Map.of(),
                            rs.getString("display_name"),
                            rs.getString("avatar_url")
                    );
                }
        );
    }

    @Override
    public boolean existsAccountKey(String accountKey) {
        Integer exists = repository.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?", Integer.class, accountKey);
        return exists != null && exists > 0;
    }

    @Override
    public boolean renameAccountKey(String oldAccountKey, String newAccountKey) {
        int updated = repository.update("UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?", newAccountKey, oldAccountKey);
        return updated == 1;
    }

    @Override
    public void saveStorageState(String accountKey, String userId, String nickname, String storageState) {
        repository.update(
                """
                INSERT INTO uploader_account_douyin (account_key, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                accountKey,
                userId,
                nickname,
                storageState
        );
    }

    @Override
    public Optional<String> findStorageState(String accountKey) {
        List<String> values = repository.query(
                "SELECT storage_state_json FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getString("storage_state_json"),
                accountKey
        );
        if (values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            return Optional.empty();
        }
        return Optional.of(values.get(0));
    }

    @Override
    public Optional<LocalDateTime> findUpdatedAt(String accountKey) {
        List<LocalDateTime> values = repository.query(
                "SELECT updated_at FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getTimestamp("updated_at").toLocalDateTime(),
                accountKey
        );
        return values.stream().findFirst();
    }

    @Override
    public SocialAccountProfile findProfile(String accountKey) {
        List<SocialAccountProfile> values = repository.query(
                "SELECT user_id, nickname FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> new SocialAccountProfile(rs.getString("user_id"), rs.getString("nickname")),
                accountKey
        );
        return values.stream().findFirst().orElse(new SocialAccountProfile(null, null));
    }

    @Override
    public Optional<String> findLatestAccountKeyByUserId(String userId) {
        List<String> existing = repository.query(
                "SELECT account_key FROM " + TABLE + " WHERE user_id = ? ORDER BY updated_at DESC LIMIT 1",
                (rs, rowNum) -> rs.getString("account_key"),
                userId
        );
        return existing.stream().findFirst();
    }

    private Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
