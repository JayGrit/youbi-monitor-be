package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.BilibiliPlaywrightAccountStatus;
import com.youbi.monitor.model.BilibiliAccountProfile;
import com.youbi.monitor.repository.BilibiliPlaywrightAccountRepository;
import com.youbi.monitor.repository.IBilibiliPlaywrightAccountRepositoryService;
import com.youbi.monitor.repository.RepositorySchemaSupport;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class BilibiliPlaywrightAccountRepositoryServiceImpl implements IBilibiliPlaywrightAccountRepositoryService {
    private static final String TABLE = "uploader_account_bilibili";

    private final BilibiliPlaywrightAccountRepository repository;

    public BilibiliPlaywrightAccountRepositoryServiceImpl(BilibiliPlaywrightAccountRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
        RepositorySchemaSupport.ensureSurrogatePrimaryKey(repository, TABLE);
        RepositorySchemaSupport.ensureColumn(repository, TABLE, "playwright_mid", "BIGINT NULL");
        RepositorySchemaSupport.ensureColumn(repository, TABLE, "playwright_uname", "VARCHAR(128) NULL");
        RepositorySchemaSupport.ensureColumn(repository, TABLE, "playwright_storage_state_json", "MEDIUMTEXT NULL");
        RepositorySchemaSupport.ensureColumn(repository, TABLE, "playwright_updated_at", "DATETIME NULL");
    }

    @Override
    public List<BilibiliPlaywrightAccountStatus> listAccounts(AccountAvailabilityResolver availabilityResolver, AccountEnabledResolver enabledResolver) {
        return repository.query(
                """
                SELECT account_key, mid, uname, playwright_mid, playwright_uname, playwright_storage_state_json, playwright_updated_at
                FROM uploader_account_bilibili
                ORDER BY account_key
                """,
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String storageState = rs.getString("playwright_storage_state_json");
                    AccountAvailability availability = availabilityResolver.resolve(accountKey);
                    return new BilibiliPlaywrightAccountStatus(
                            "database",
                            accountKey,
                            storageState != null && !storageState.isBlank(),
                            storageState == null ? 0 : storageState.getBytes(StandardCharsets.UTF_8).length,
                            rs.getTimestamp("playwright_updated_at") == null ? null : rs.getTimestamp("playwright_updated_at").toLocalDateTime(),
                            rs.getObject("playwright_mid") == null ? (rs.getObject("mid") == null ? null : rs.getLong("mid")) : rs.getLong("playwright_mid"),
                            blankToNull(rs.getString("playwright_uname")) == null ? rs.getString("uname") : rs.getString("playwright_uname"),
                            availability.lastUploadAt(),
                            availability.nextUploadAllowedAt(),
                            availability.todayUploadCount(),
                            availability.cooldownWaitingCount(),
                            availability.uploadRunningCount(),
                            enabledResolver.enabled(accountKey),
                            null,
                            "已保存",
                            Map.of()
                    );
                }
        );
    }

    @Override
    public Optional<String> findStorageState(String accountKey) {
        List<String> values = repository.query(
                "SELECT playwright_storage_state_json FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getString("playwright_storage_state_json"),
                accountKey
        );
        if (values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            return Optional.empty();
        }
        return Optional.of(values.get(0));
    }

    @Override
    public BilibiliAccountProfile findProfile(String accountKey) {
        List<BilibiliAccountProfile> values = repository.query(
                "SELECT mid, uname, playwright_mid, playwright_uname FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> new BilibiliAccountProfile(
                        rs.getObject("playwright_mid") == null ? (rs.getObject("mid") == null ? null : rs.getLong("mid")) : rs.getLong("playwright_mid"),
                        blankToNull(rs.getString("playwright_uname")) == null ? rs.getString("uname") : rs.getString("playwright_uname")
                ),
                accountKey
        );
        return values.stream().findFirst().orElse(new BilibiliAccountProfile(null, null));
    }

    @Override
    public Optional<LocalDateTime> findUpdatedAt(String accountKey) {
        List<LocalDateTime> values = repository.query(
                "SELECT playwright_updated_at FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getTimestamp("playwright_updated_at") == null ? null : rs.getTimestamp("playwright_updated_at").toLocalDateTime(),
                accountKey
        );
        return values.stream().findFirst();
    }

    @Override
    public boolean updateStorageState(String accountKey, Long mid, String uname, String storageState) {
        int updated = repository.update(
                """
                UPDATE uploader_account_bilibili
                SET playwright_mid = ?, playwright_uname = ?, playwright_storage_state_json = ?, playwright_updated_at = NOW()
                WHERE account_key = ?
                """,
                mid,
                uname,
                storageState,
                accountKey
        );
        return updated > 0;
    }

    private String blankToNull(String value) {
        String text = value == null ? "" : value.trim();
        return text.isBlank() ? null : text;
    }
}
