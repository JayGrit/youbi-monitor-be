package com.youbi.monitor.service;

import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

@Service
public class AccountSchemaService {
    private final MonitorRepository repository;

    public AccountSchemaService(MonitorRepository repository) {
        this.repository = repository;
    }

    boolean columnExists(String table, String column) {
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

    boolean tableExists(String table) {
        Integer count = repository.queryForObject(
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

    void ensureDownloaderStagingColumns() {
        if (!tableExists("uploader_account")) {
            return;
        }
        if (!columnExists("uploader_account", "downloader_max_staged_count")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN downloader_max_staged_count INT NOT NULL DEFAULT 5
                    """
            );
        }
    }

    void ensureQuietTimeColumns() {
        if (!tableExists("uploader_account")) {
            return;
        }
        if (!columnExists("uploader_account", "upload_quiet_start_time")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN upload_quiet_start_time TIME NOT NULL DEFAULT '01:00:00'
                    """
            );
        }
        if (!columnExists("uploader_account", "upload_quiet_end_time")) {
            repository.update(
                    """
                    ALTER TABLE uploader_account
                    ADD COLUMN upload_quiet_end_time TIME NOT NULL DEFAULT '07:00:00'
                    """
            );
        }
    }

    void ensureDeprecatedColumn() {
        if (!tableExists("uploader_account") || columnExists("uploader_account", "is_deprecated")) {
            return;
        }
        repository.update("""
                ALTER TABLE uploader_account
                ADD COLUMN is_deprecated TINYINT(1) NOT NULL DEFAULT 0
                """);
    }

    void ensureLatestVideoPublishColumns() {
        if (!tableExists("uploader_account")) {
            return;
        }
        if (!columnExists("uploader_account", "latest_video_publish_at")) {
            repository.update("""
                    ALTER TABLE uploader_account
                    ADD COLUMN latest_video_publish_at DATETIME NULL
                    """);
        }
        if (!columnExists("uploader_account", "latest_video_publish_at_epoch")) {
            repository.update("""
                    ALTER TABLE uploader_account
                    ADD COLUMN latest_video_publish_at_epoch BIGINT NULL
                    """);
        }
    }

    void ensureOperatorLoginstateTables() {
        if (!tableExists("operator_profile")) {
            repository.update("""
                    CREATE TABLE operator_profile (
                      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                      profile_key VARCHAR(64) NOT NULL,
                      profile_path VARCHAR(1024) NOT NULL,
                      enabled TINYINT(1) NOT NULL DEFAULT 1,
                      note VARCHAR(255) NULL,
                      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      PRIMARY KEY (id),
                      UNIQUE KEY uniq_operator_profile_key (profile_key)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """);
        }
        if (!tableExists("operator_loginstate")) {
            repository.update("""
                    CREATE TABLE operator_loginstate (
                      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                      platform VARCHAR(64) NOT NULL,
                      topic VARCHAR(128) NOT NULL,
                      account_category VARCHAR(32) NOT NULL DEFAULT 'video_platform',
                      login_state_type VARCHAR(32) NOT NULL,
                      storage_state_json MEDIUMTEXT NULL,
                      profile_id BIGINT UNSIGNED NULL,
                      video_generation_quota INT NOT NULL DEFAULT 0,
                      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      PRIMARY KEY (id),
                      UNIQUE KEY uniq_operator_loginstate_platform_key (platform, topic),
                      KEY idx_operator_loginstate_category_platform (account_category, platform),
                      KEY idx_operator_loginstate_profile_id (profile_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """);
        }
        if (tableExists("uploader_phone_account")) {
            if (!columnExists("uploader_phone_account", "display_name")) {
                repository.update("ALTER TABLE uploader_phone_account ADD COLUMN display_name VARCHAR(128) NULL AFTER disabled");
            }
            if (!columnExists("uploader_phone_account", "avatar_url")) {
                repository.update("ALTER TABLE uploader_phone_account ADD COLUMN avatar_url VARCHAR(1024) NULL AFTER display_name");
            }
        }
    }
}
