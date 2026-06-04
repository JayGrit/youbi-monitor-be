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
    public int updateDisplayName(String table, String accountKey, String displayName) {
        return repository.update(
                "UPDATE " + table + " SET display_name = ?, updated_at = NOW() WHERE account_key = ?",
                displayName,
                accountKey
        );
    }

    @Override
    public int updateAvatarUrl(String table, String accountKey, String avatarUrl) {
        return repository.update(
                "UPDATE " + table + " SET avatar_url = ?, updated_at = NOW() WHERE account_key = ?",
                avatarUrl,
                accountKey
        );
    }

    @Override
    public AccountProfileUpdateResult findProfile(String table, String accountKey) {
        return repository.queryForObject(
                "SELECT display_name, avatar_url FROM " + table + " WHERE account_key = ?",
                (rs, rowNum) -> new AccountProfileUpdateResult(rs.getString("display_name"), rs.getString("avatar_url")),
                accountKey
        );
    }

    @Override
    public void ensureProfileColumns(String table) {
        ensureColumn(table, "display_name", "VARCHAR(128) NULL");
        ensureColumn(table, "avatar_url", "VARCHAR(1024) NULL");
    }

    private void ensureColumn(String table, String column, String definition) {
        Integer count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        if (count == null || count == 0) {
            repository.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
    }
}
