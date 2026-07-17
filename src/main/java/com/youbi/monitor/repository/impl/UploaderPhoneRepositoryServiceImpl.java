package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.UploaderPhoneAccountUpdateRequest;
import com.youbi.monitor.model.UploaderPhoneAccountOption;
import com.youbi.monitor.model.UploaderPhoneBinding;
import com.youbi.monitor.model.UploaderPhoneMatrixResponse;
import com.youbi.monitor.model.UploaderPhonePlatformAccounts;
import com.youbi.monitor.model.UploaderPhoneRecord;
import com.youbi.monitor.repository.IUploaderPhoneRepositoryService;
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
            new PlatformTable("douyin"),
            new PlatformTable("xiaohongshu"),
            new PlatformTable("bilibili"),
            new PlatformTable("shipinhao"),
            new PlatformTable("kuaishou"),
            new PlatformTable("jinritoutiao"),
            new PlatformTable("x"),
            new PlatformTable("youtube"),
            new PlatformTable("doubao"),
            new PlatformTable("notebooklm"),
            new PlatformTable("chatgpt")
    );

    private final UploaderPhoneRepository repository;

    public UploaderPhoneRepositoryServiceImpl(UploaderPhoneRepository repository) {
        this.repository = repository;
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
        if (!tableExists("operator_loginstate")) {
            return List.of();
        }
        ensureUploaderPhoneAccountColumn("display_name", "VARCHAR(128) NULL");
        ensureUploaderPhoneAccountColumn("avatar_url", "VARCHAR(1024) NULL");
        return repository.query(
                """
                SELECT account.id,
                       account.account_key,
                       account.account_key AS resolved_display_name,
                       phone_profile.display_name AS remark,
                       phone_profile.avatar_url,
                       state.is_available
                FROM operator_loginstate account
                LEFT JOIN uploader_account state
                  ON state.platform = account.platform
                 AND state.account_key = account.account_key
                LEFT JOIN (
                    SELECT platform, account_id,
                           MAX(display_name) AS display_name,
                           MAX(avatar_url) AS avatar_url
                    FROM uploader_phone_account
                    GROUP BY platform, account_id
                ) phone_profile
                  ON phone_profile.platform = account.platform
                 AND phone_profile.account_id = account.id
                WHERE account.platform = ?
                ORDER BY account.account_key
                """,
                (rs, rowNum) -> new UploaderPhoneAccountOption(
                        rs.getLong("id"),
                        rs.getString("account_key"),
                        rs.getString("resolved_display_name"),
                        rs.getString("remark"),
                        rs.getString("avatar_url"),
                        rs.getObject("is_available", Boolean.class)
                ),
                platform.key()
        );
    }

    private boolean accountIdExists(PlatformTable platform, long accountId) {
        if (!tableExists("operator_loginstate")) {
            return false;
        }
        Integer count = repository.queryForObject(
                "SELECT COUNT(*) FROM operator_loginstate WHERE platform = ? AND id = ?",
                Integer.class,
                platform.key(),
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
        if (tableExists(ACCOUNT_TABLE)) {
            ensureUploaderPhoneAccountColumn("display_name", "VARCHAR(128) NULL");
            ensureUploaderPhoneAccountColumn("avatar_url", "VARCHAR(1024) NULL");
        }
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
        if (!columnExists(ACCOUNT_TABLE, column)) {
            repository.update("ALTER TABLE " + ACCOUNT_TABLE + " ADD COLUMN " + column + " " + definition);
        }
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

    private void dropColumnIfExists(String table, String column) {
    }

    private void ensureAccountColumn(String table, String column, String definition) {
    }

    private boolean tableExists(String table) {
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

    private record PlatformTable(String key) {
    }
}
