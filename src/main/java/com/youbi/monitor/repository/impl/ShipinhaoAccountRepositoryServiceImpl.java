package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.ShipinhaoAccountStatus;
import com.youbi.monitor.model.SocialAccountProfile;
import com.youbi.monitor.repository.IShipinhaoAccountRepositoryService;
import com.youbi.monitor.repository.ShipinhaoAccountRepository;
import com.youbi.monitor.repository.RepositorySchemaSupport;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ShipinhaoAccountRepositoryServiceImpl implements IShipinhaoAccountRepositoryService {
    private static final String TABLE = "uploader_account_shipinhao";

    private final ShipinhaoAccountRepository repository;

    public ShipinhaoAccountRepositoryServiceImpl(ShipinhaoAccountRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
    }

    @Override
    public List<ShipinhaoAccountStatus> listAccounts() {
        String runningCountSql = runningCountSql();
        return repository.query(
                ("""
                SELECT ua.account_key, ua.last_upload_at, ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds, ua.upload_cooldown_max_seconds,
                       ua.today_upload_count, ua.cooldown_waiting_count, %s,
                       ua.is_enabled,
                       pa.user_id, pa.nickname, pa.storage_state_json, pa.updated_at, pa.display_name, pa.avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_shipinhao pa ON pa.account_key = ua.account_key
                WHERE ua.platform = 'shipinhao'
                ORDER BY ua.account_key
                """).formatted(runningCountSql),
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new ShipinhaoAccountStatus(
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

    private String runningCountSql() {
        return columnExists("uploader_account", "upload_running_task_id")
                ? "CASE WHEN NULLIF(ua.upload_running_task_id, '') IS NULL THEN 0 ELSE 1 END AS upload_running_count"
                : "ua.upload_running_count";
    }

    private boolean columnExists(String table, String column) {
        Integer count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        return count != null && count > 0;
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
                INSERT INTO uploader_account_shipinhao (account_key, user_id, nickname, storage_state_json, is_available, updated_at)
                VALUES (?, ?, ?, ?, 1, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), is_available = 1, updated_at = NOW()
                """,
                accountKey,
                userId,
                nickname,
                storageState
        );
    }

    @Override
    public void markUnavailable(String accountKey) {
        repository.update(
                "UPDATE " + TABLE + " SET is_available = 0, updated_at = NOW() WHERE account_key = ?",
                accountKey
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

    private Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
