package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.UploaderPhoneAccountUpdateRequest;
import com.youbi.monitor.model.UploaderPhoneAccountOption;
import com.youbi.monitor.model.UploaderPhoneBinding;
import com.youbi.monitor.model.UploaderPhoneMatrixResponse;
import com.youbi.monitor.model.UploaderPhonePlatformAccounts;
import com.youbi.monitor.model.UploaderPhoneRecord;
import com.youbi.monitor.repository.IUploaderPhoneRepositoryService;
import com.youbi.monitor.repository.RepositorySchemaSupport;
import com.youbi.monitor.repository.UploaderPhoneRepository;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UploaderPhoneRepositoryServiceImpl implements IUploaderPhoneRepositoryService {
    private static final String TABLE = "uploader_phone";
    private static final String ACCOUNT_TABLE = "uploader_phone_account";
    private static final List<PlatformTable> PLATFORMS = List.of(
            new PlatformTable("douyin", "uploader_account_douyin", "douyin_account_id"),
            new PlatformTable("xiaohongshu", "uploader_account_xiaohongshu", "xiaohongshu_account_id"),
            new PlatformTable("bilibili", "uploader_account_bilibili", "bilibili_account_id"),
            new PlatformTable("shipinhao", "uploader_account_shipinhao", "shipinhao_account_id"),
            new PlatformTable("kuaishou", "uploader_account_kuaishou", "kuaishou_account_id"),
            new PlatformTable("jinritoutiao", "uploader_account_jinritoutiao", "jinritoutiao_account_id")
    );

    private final UploaderPhoneRepository repository;

    public UploaderPhoneRepositoryServiceImpl(UploaderPhoneRepository repository) {
        this.repository = repository;
        ensureSchema();
    }

    public UploaderPhoneMatrixResponse matrix() {
        return new UploaderPhoneMatrixResponse(phones(), platformAccounts());
    }

    public UploaderPhoneRecord updatePlatformAccount(long phoneId, String platform, UploaderPhoneAccountUpdateRequest request) throws IOException {
        PlatformTable platformTable = platformTable(platform);
        Long accountId = request == null ? null : request.accountId();
        String note = request == null ? "" : text(request.note());
        boolean disabled = request != null && Boolean.TRUE.equals(request.disabled());
        if (accountId != null && accountId <= 0) {
            accountId = null;
        }
        if (accountId != null && !accountIdExists(platformTable, accountId)) {
            throw new IOException("Uploader account not found: " + platform + "/" + accountId);
        }
        if (!phoneExists(phoneId)) {
            throw new IOException("Uploader phone not found: " + phoneId);
        }
        if (accountId == null && !disabled && note.isBlank()) {
            repository.update(
                    "DELETE FROM uploader_phone_account WHERE phone_id = ? AND platform = ?",
                    phoneId,
                    platformTable.key()
            );
        } else {
            repository.update(
                    """
                    INSERT INTO uploader_phone_account (phone_id, platform, account_id, note, disabled, updated_at)
                    VALUES (?, ?, ?, ?, ?, NOW())
                    ON DUPLICATE KEY UPDATE account_id = VALUES(account_id), note = VALUES(note), disabled = VALUES(disabled), updated_at = NOW()
                    """,
                    phoneId,
                    platformTable.key(),
                    accountId,
                    note.isBlank() ? null : note,
                    disabled
            );
        }
        return phone(phoneId);
    }

    private List<UploaderPhoneRecord> phones() {
        Map<Long, Map<String, UploaderPhoneBinding>> bindingsByPhone = phoneAccountBindings();
        return repository.query(
                """
                SELECT id, phone
                     , remark
                     , note
                FROM uploader_phone
                ORDER BY id
                """,
                (rs, rowNum) -> new UploaderPhoneRecord(
                        rs.getLong("id"),
                        rs.getString("phone"),
                        rs.getString("remark"),
                        rs.getString("note"),
                        accountsOnly(bindingsByPhone.getOrDefault(rs.getLong("id"), Map.of())),
                        bindingsByPhone.getOrDefault(rs.getLong("id"), Map.of())
                )
        );
    }

    private UploaderPhoneRecord phone(long phoneId) throws IOException {
        Map<Long, Map<String, UploaderPhoneBinding>> bindingsByPhone = phoneAccountBindings();
        List<UploaderPhoneRecord> rows = repository.query(
                """
                SELECT id, phone
                     , remark
                     , note
                FROM uploader_phone
                WHERE id = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new UploaderPhoneRecord(
                        rs.getLong("id"),
                        rs.getString("phone"),
                        rs.getString("remark"),
                        rs.getString("note"),
                        accountsOnly(bindingsByPhone.getOrDefault(rs.getLong("id"), Map.of())),
                        bindingsByPhone.getOrDefault(rs.getLong("id"), Map.of())
                ),
                phoneId
        );
        return rows.stream().findFirst().orElseThrow(() -> new IOException("Uploader phone not found: " + phoneId));
    }

    private Map<Long, Map<String, UploaderPhoneBinding>> phoneAccountBindings() {
        Map<Long, Map<String, UploaderPhoneBinding>> result = new HashMap<>();
        repository.query(
                """
                SELECT phone_id, platform, account_id, note, disabled
                FROM uploader_phone_account
                ORDER BY phone_id, platform
                """,
                rs -> {
                    result.computeIfAbsent(rs.getLong("phone_id"), ignored -> new HashMap<>())
                            .put(rs.getString("platform"), new UploaderPhoneBinding(
                                    rs.getObject("account_id", Long.class),
                                    rs.getString("note"),
                                    rs.getBoolean("disabled")
                            ));
                }
        );
        return result;
    }

    private Map<String, Long> accountsOnly(Map<String, UploaderPhoneBinding> bindings) {
        Map<String, Long> result = new HashMap<>();
        bindings.forEach((platform, binding) -> {
            if (binding.accountId() != null) {
                result.put(platform, binding.accountId());
            }
        });
        return result;
    }

    private List<UploaderPhonePlatformAccounts> platformAccounts() {
        return PLATFORMS.stream()
                .map(platform -> new UploaderPhonePlatformAccounts(platform.key(), accountOptions(platform)))
                .toList();
    }

    private List<UploaderPhoneAccountOption> accountOptions(PlatformTable platform) {
        if (!tableExists(platform.table())) {
            return List.of();
        }
        RepositorySchemaSupport.ensureSurrogatePrimaryKey(repository, platform.table());
        ensureAccountColumn(platform.table(), "display_name", "VARCHAR(128) NULL");
        ensureAccountColumn(platform.table(), "avatar_url", "VARCHAR(1024) NULL");
        String nameExpression = "COALESCE(NULLIF(nickname, ''), account_key)";
        if ("bilibili".equals(platform.key())) {
            nameExpression = "COALESCE(NULLIF(uname, ''), account_key)";
        }
        return repository.query(
                ("""
                SELECT id, account_key, %s AS display_name, display_name AS remark, avatar_url
                FROM %s
                ORDER BY account_key
                """).formatted(nameExpression, platform.table()),
                (rs, rowNum) -> new UploaderPhoneAccountOption(
                        rs.getLong("id"),
                        rs.getString("account_key"),
                        rs.getString("display_name"),
                        rs.getString("remark"),
                        rs.getString("avatar_url")
                )
        );
    }

    private boolean accountIdExists(PlatformTable platform, long accountId) {
        if (!tableExists(platform.table())) {
            return false;
        }
        RepositorySchemaSupport.ensureSurrogatePrimaryKey(repository, platform.table());
        Integer count = repository.queryForObject(
                "SELECT COUNT(*) FROM " + platform.table() + " WHERE id = ?",
                Integer.class,
                accountId
        );
        return count != null && count > 0;
    }

    private boolean phoneExists(long phoneId) {
        Integer count = repository.queryForObject(
                "SELECT COUNT(*) FROM uploader_phone WHERE id = ?",
                Integer.class,
                phoneId
        );
        return count != null && count > 0;
    }

    @Override
    public void ensureSchema() {
    }

    private void migrateLegacyAccountColumns() {
    }

    private void dropLegacyPhoneColumns() {
    }

    private void seedPhone(String phone, String remark) {
        repository.update(
                """
                INSERT INTO uploader_phone (phone, remark, note, updated_at)
                VALUES (?, ?, NULL, NOW())
                ON DUPLICATE KEY UPDATE remark = VALUES(remark)
                """,
                phone,
                remark
        );
    }

    private void ensureUploaderPhoneColumn(String column, String definition) {
    }

    private void ensureUploaderPhoneAccountColumn(String column, String definition) {
    }

    private boolean columnExists(String table, String column) {
        return true;
    }

    private void dropColumnIfExists(String table, String column) {
    }

    private void ensureAccountColumn(String table, String column, String definition) {
    }

    private boolean tableExists(String table) {
        return true;
    }

    private PlatformTable platformTable(String platform) throws IOException {
        String normalized = text(platform);
        return PLATFORMS.stream()
                .filter(item -> item.key().equals(normalized))
                .findFirst()
                .orElseThrow(() -> new IOException("Unsupported platform: " + platform));
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record PlatformTable(String key, String table, String phoneColumn) {
    }
}
