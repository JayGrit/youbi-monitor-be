package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.AccountProfileUpdateResult;
import com.youbi.monitor.repository.AccountProfileRepository;
import com.youbi.monitor.repository.IAccountProfileRepositoryService;
import org.springframework.stereotype.Service;

@Service
public class AccountProfileRepositoryServiceImpl implements IAccountProfileRepositoryService {
    private final AccountProfileRepository repository;

    public AccountProfileRepositoryServiceImpl(AccountProfileRepository repository) {
        this.repository = repository;
    }

    @Override
    public int updateDisplayName(String platform, String topic, String displayName) {
        return repository.update(
                """
                UPDATE uploader_phone_account phone_account
                JOIN operator_loginstate loginstate
                  ON loginstate.platform = phone_account.platform
                 AND loginstate.id = phone_account.account_id
                SET phone_account.display_name = ?, phone_account.updated_at = NOW()
                WHERE loginstate.platform = ? AND loginstate.topic = ?
                """,
                displayName,
                platform,
                topic
        );
    }

    @Override
    public int updateAvatarUrl(String platform, String topic, String avatarUrl) {
        return repository.update(
                """
                UPDATE uploader_phone_account phone_account
                JOIN operator_loginstate loginstate
                  ON loginstate.platform = phone_account.platform
                 AND loginstate.id = phone_account.account_id
                SET phone_account.avatar_url = ?, phone_account.updated_at = NOW()
                WHERE loginstate.platform = ? AND loginstate.topic = ?
                """,
                avatarUrl,
                platform,
                topic
        );
    }

    @Override
    public AccountProfileUpdateResult findProfile(String platform, String topic) {
        return repository.queryForObject(
                """
                SELECT phone_account.display_name, phone_account.avatar_url
                FROM uploader_phone_account phone_account
                JOIN operator_loginstate loginstate
                  ON loginstate.platform = phone_account.platform
                 AND loginstate.id = phone_account.account_id
                WHERE loginstate.platform = ? AND loginstate.topic = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new AccountProfileUpdateResult(rs.getString("display_name"), rs.getString("avatar_url")),
                platform,
                topic
        );
    }

    @Override
    public void ensureProfileColumns(String table) {
        ensureColumn("uploader_phone_account", "display_name", "VARCHAR(128) NULL");
        ensureColumn("uploader_phone_account", "avatar_url", "VARCHAR(1024) NULL");
    }

    private void ensureColumn(String table, String column, String definition) {
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
        if (count == null || count == 0) {
            repository.update("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
    }
}
