package com.youbi.monitor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class AccountSendAvailabilityService {
    private static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_bilibili_task",
            "douyin", "uploader_douyin_task",
            "xiaohongshu", "uploader_xiaohongshu_task"
    );

    private final JdbcTemplate jdbcTemplate;

    public AccountSendAvailabilityService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public AccountSendAvailability availability(String platform, String accountKey, String platformAccountTable) {
        LocalDateTime lastUploadAt = null;
        LocalDateTime nextUploadAllowedAt = null;
        int todayUploadCount = 0;
        int cooldownWaitingCount = 0;

        if (tableExists(uploadTaskTable(platform))) {
            todayUploadCount = todayUploadCount(platform, accountKey);
            cooldownWaitingCount = platformAccountTable != null
                    && tableExists(platformAccountTable)
                    && columnExists(platformAccountTable, "next_upload_allowed_at")
                    ? cooldownWaitingCount(platform, accountKey, platformAccountTable)
                    : 0;
        }

        if (platformAccountTable != null
                && tableExists(platformAccountTable)
                && columnExists(platformAccountTable, "next_upload_allowed_at")) {
            Optional<AccountSendAvailability> platformSpecific = queryPlatformAccount(platformAccountTable, accountKey);
            if (platformSpecific.isPresent()) {
                lastUploadAt = latest(lastUploadAt, platformSpecific.get().lastUploadAt());
                nextUploadAllowedAt = latest(nextUploadAllowedAt, platformSpecific.get().nextUploadAllowedAt());
            }
        }

        return new AccountSendAvailability(lastUploadAt, nextUploadAllowedAt, todayUploadCount, cooldownWaitingCount);
    }

    private Optional<AccountSendAvailability> queryPlatformAccount(String table, String accountKey) {
        String lastUploadSelect = columnExists(table, "last_upload_at") ? "last_upload_at" : "NULL";
        List<AccountSendAvailability> rows = jdbcTemplate.query(
                "SELECT " + lastUploadSelect + " AS last_upload_at, next_upload_allowed_at FROM " + table + " WHERE account_key = ? LIMIT 1",
                (rs, rowNum) -> new AccountSendAvailability(
                        toLocalDateTime(rs.getTimestamp("last_upload_at")),
                        toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                        0,
                        0
                ),
                accountKey
        );
        return rows.stream().findFirst();
    }

    private int todayUploadCount(String platform, String accountKey) {
        String uploadTaskTable = uploadTaskTable(platform);
        Integer count = jdbcTemplate.queryForObject(
                ("""
                SELECT COUNT(*)
                FROM %s
                WHERE account_key = ?
                  AND status = 'success'
                  AND completed_at >= DATE_ADD(
                      CASE
                          WHEN TIME(NOW()) >= '08:00:00' THEN CURDATE()
                          ELSE DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                      END,
                      INTERVAL 8 HOUR
                  )
                  AND completed_at < DATE_ADD(
                      CASE
                          WHEN TIME(NOW()) >= '08:00:00' THEN DATE_ADD(CURDATE(), INTERVAL 1 DAY)
                          ELSE CURDATE()
                      END,
                      INTERVAL 2 HOUR
                  )
                """).formatted(uploadTaskTable),
                Integer.class,
                accountKey
        );
        return count == null ? 0 : count;
    }

    private int cooldownWaitingCount(String platform, String accountKey, String platformAccountTable) {
        String uploadTaskTable = uploadTaskTable(platform);
        Integer count = jdbcTemplate.queryForObject(
                ("""
                SELECT COUNT(*)
                FROM %s s
                JOIN %s a
                  ON a.account_key = s.account_key
                WHERE s.account_key = ?
                  AND s.status = 'ready'
                  AND a.next_upload_allowed_at > NOW()
                """).formatted(uploadTaskTable, platformAccountTable),
                Integer.class,
                accountKey
        );
        return count == null ? 0 : count;
    }

    private String uploadTaskTable(String platform) {
        return UPLOADER_TASK_TABLES.getOrDefault(platform, "");
    }

    private boolean tableExists(String table) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                """,
                Integer.class,
                table
        );
        return count != null && count > 0;
    }

    private boolean columnExists(String table, String column) {
        Integer count = jdbcTemplate.queryForObject(
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

    private static LocalDateTime latest(LocalDateTime left, LocalDateTime right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return left.isAfter(right) ? left : right;
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
