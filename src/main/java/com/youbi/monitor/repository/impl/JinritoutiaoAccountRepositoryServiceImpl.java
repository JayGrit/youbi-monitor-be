package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.JinritoutiaoAccountStatus;
import com.youbi.monitor.model.SocialAccountProfile;
import com.youbi.monitor.repository.IJinritoutiaoAccountRepositoryService;
import com.youbi.monitor.repository.JinritoutiaoAccountRepository;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class JinritoutiaoAccountRepositoryServiceImpl implements IJinritoutiaoAccountRepositoryService {
    private static final String TABLE = "uploader_account_jinritoutiao";

    private final JinritoutiaoAccountRepository repository;

    public JinritoutiaoAccountRepositoryServiceImpl(JinritoutiaoAccountRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
    }

    @Override
    public List<JinritoutiaoAccountStatus> listAccounts() {
        return repository.query(
                """
                SELECT ua.topic, ua.last_upload_at, ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds, ua.upload_cooldown_max_seconds,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ua.platform
                           AND upload_submission.topic = ua.topic
                           AND upload_submission.status = 'success'
                           AND DATE(upload_submission.completed_at) = CURDATE()
                       ) today_upload_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ua.platform
                           AND upload_submission.topic = ua.topic
                           AND upload_submission.status = 'ready'
                       ) cooldown_waiting_count,
                       (
                         SELECT COUNT(*)
                         FROM uploader_task upload_submission
                         WHERE upload_submission.platform = ua.platform
                           AND upload_submission.topic = ua.topic
                           AND upload_submission.status = 'running'
                       ) upload_running_count,
                       ua.is_enabled,
                       pa.user_id, pa.nickname, pa.storage_state_json, pa.updated_at, pa.display_name, pa.avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_jinritoutiao pa ON pa.topic = ua.topic
                WHERE ua.platform = 'jinritoutiao' AND ua.is_deprecated = 0
                ORDER BY ua.topic
                """,
                (rs, rowNum) -> {
                    String topic = rs.getString("topic");
                    String json = rs.getString("storage_state_json");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new JinritoutiaoAccountStatus(
                            "database",
                            topic,
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
    public boolean existsTopic(String topic) {
        Integer exists = repository.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE topic = ?", Integer.class, topic);
        return exists != null && exists > 0;
    }

    @Override
    public boolean renameTopic(String oldTopic, String newTopic) {
        int updated = repository.update("UPDATE " + TABLE + " SET topic = ?, updated_at = NOW() WHERE topic = ?", newTopic, oldTopic);
        return updated == 1;
    }

    @Override
    public void saveStorageState(String topic, String userId, String nickname, String storageState) {
        repository.update(
                """
                INSERT INTO uploader_account_jinritoutiao (topic, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                topic,
                userId,
                nickname,
                storageState
        );
    }

    @Override
    public Optional<String> findStorageState(String topic) {
        List<String> values = repository.query(
                "SELECT storage_state_json FROM " + TABLE + " WHERE topic = ?",
                (rs, rowNum) -> rs.getString("storage_state_json"),
                topic
        );
        if (values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            return Optional.empty();
        }
        return Optional.of(values.get(0));
    }

    @Override
    public Optional<LocalDateTime> findUpdatedAt(String topic) {
        List<LocalDateTime> values = repository.query(
                "SELECT updated_at FROM " + TABLE + " WHERE topic = ?",
                (rs, rowNum) -> rs.getTimestamp("updated_at").toLocalDateTime(),
                topic
        );
        return values.stream().findFirst();
    }

    @Override
    public SocialAccountProfile findProfile(String topic) {
        List<SocialAccountProfile> values = repository.query(
                "SELECT user_id, nickname FROM " + TABLE + " WHERE topic = ?",
                (rs, rowNum) -> new SocialAccountProfile(rs.getString("user_id"), rs.getString("nickname")),
                topic
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
