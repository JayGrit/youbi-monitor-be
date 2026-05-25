package com.youbi.monitor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class AccountSendAvailabilityService {
    private final JdbcTemplate jdbcTemplate;

    public AccountSendAvailabilityService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public AccountSendAvailability availability(String platform, String accountKey, String platformAccountTable) {
        LocalDateTime lastUploadAt = null;
        LocalDateTime nextUploadAllowedAt = null;
        int todayUploadCount = 0;
        int cooldownWaitingCount = 0;

        boolean uploadAccountExists = tableExists("yd_upload_account");
        if (uploadAccountExists) {
            Optional<AccountSendAvailability> shared = querySharedUploadAccount(platform, accountKey);
            if (shared.isPresent()) {
                lastUploadAt = latest(lastUploadAt, shared.get().lastUploadAt());
                nextUploadAllowedAt = latest(nextUploadAllowedAt, shared.get().nextUploadAllowedAt());
            }
        }

        if (tableExists("yd_upload_submission")) {
            todayUploadCount = todayUploadCount(platform, accountKey);
            cooldownWaitingCount = uploadAccountExists ? cooldownWaitingCount(platform, accountKey) : 0;
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

    private Optional<AccountSendAvailability> querySharedUploadAccount(String platform, String accountKey) {
        List<AccountSendAvailability> rows = jdbcTemplate.query(
                """
                SELECT last_upload_at, next_upload_allowed_at
                FROM yd_upload_account
                WHERE platform = ? AND account_key = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new AccountSendAvailability(
                        toLocalDateTime(rs.getTimestamp("last_upload_at")),
                        toLocalDateTime(rs.getTimestamp("next_upload_allowed_at")),
                        0,
                        0
                ),
                platform,
                accountKey
        );
        return rows.stream().findFirst();
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
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM yd_upload_submission
                WHERE platform = ?
                  AND account_key = ?
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
                """,
                Integer.class,
                platform,
                accountKey
        );
        return count == null ? 0 : count;
    }

    private int cooldownWaitingCount(String platform, String accountKey) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM yd_upload_submission s
                JOIN yd_upload_account a
                  ON a.platform = s.platform AND a.account_key = s.account_key
                WHERE s.platform = ?
                  AND s.account_key = ?
                  AND s.status = 'ready'
                  AND a.next_upload_allowed_at > NOW()
                """,
                Integer.class,
                platform,
                accountKey
        );
        return count == null ? 0 : count;
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
