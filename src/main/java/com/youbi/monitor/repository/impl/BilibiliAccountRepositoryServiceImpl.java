package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.BilibiliAccountStatus;
import com.youbi.monitor.repository.BilibiliAccountRepository;
import com.youbi.monitor.repository.IBilibiliAccountRepositoryService;
import com.youbi.monitor.repository.RepositorySchemaSupport;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class BilibiliAccountRepositoryServiceImpl implements IBilibiliAccountRepositoryService {
    private static final String TABLE = "uploader_account_bilibili";

    private final BilibiliAccountRepository repository;

    public BilibiliAccountRepositoryServiceImpl(BilibiliAccountRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
    }

    @Override
    public List<BilibiliAccountStatus> listAccounts() {
        return repository.query(
                """
                SELECT ua.account_key, ua.last_upload_at, ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds, ua.upload_cooldown_max_seconds,
                       ua.today_upload_count, ua.cooldown_waiting_count, ua.upload_running_count,
                       ua.is_enabled,
                       pa.mid, pa.uname, pa.login_info_json, pa.updated_at, pa.display_name, pa.avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_bilibili pa ON pa.account_key = ua.account_key
                WHERE ua.platform = 'bilibili'
                ORDER BY ua.account_key
                """,
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("login_info_json");
                    Long mid = rs.getObject("mid") == null ? null : rs.getLong("mid");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new BilibiliAccountStatus(
                            "database",
                            accountKey,
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            updatedAt,
                            mid,
                            rs.getString("uname"),
                            null,
                            null,
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
        Integer count = repository.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?",
                Integer.class,
                accountKey
        );
        return count != null && count > 0;
    }

    @Override
    public boolean renameAccountKey(String oldAccountKey, String newAccountKey) {
        int updated = repository.update(
                "UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?",
                newAccountKey,
                oldAccountKey
        );
        return updated == 1;
    }

    @Override
    public Optional<String> findLatestAccountKeyByMid(long mid) {
        List<String> existing = repository.query(
                "SELECT account_key FROM " + TABLE + " WHERE mid = ? ORDER BY updated_at DESC LIMIT 1",
                (rs, rowNum) -> rs.getString("account_key"),
                mid
        );
        return existing.stream().findFirst();
    }

    @Override
    public Optional<String> findLoginInfoJson(String accountKey) {
        List<String> values = repository.query(
                "SELECT login_info_json FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getString("login_info_json"),
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
    public void saveLoginInfo(String accountKey, Long mid, String uname, String loginInfoJson) {
        repository.update(
                """
                INSERT INTO uploader_account_bilibili (account_key, mid, uname, login_info_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE mid = VALUES(mid), uname = VALUES(uname), login_info_json = VALUES(login_info_json), updated_at = NOW()
                """,
                accountKey,
                mid,
                uname,
                loginInfoJson
        );
    }

    private Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
