package com.youbi.monitor.service;

import com.youbi.monitor.dto.AccountSendAvailability;
import com.youbi.monitor.dto.ShipinhaoAccountStatus;
import com.youbi.monitor.dto.UploaderAccountState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.youbi.monitor.repository.ShipinhaoAccountRepository;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ShipinhaoAccountService {
    static final String DEFAULT_ACCOUNT_KEY = "default";
    static final String HOME_URL = "https://channels.weixin.qq.com/platform";
    static final String PUBLISH_VIDEO_URL = "https://channels.weixin.qq.com/platform/post/create";
    static final String MANAGE_URL = "https://channels.weixin.qq.com/platform/post/list";

    private static final String TABLE = "uploader_account_shipinhao";

    private final ShipinhaoAccountRepository repository;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final SocialBrowserFactory browserFactory;
    private final UploaderAccountService uploaderAccountService;

    public ShipinhaoAccountService(
            ShipinhaoAccountRepository repository,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            SocialBrowserFactory browserFactory,
            UploaderAccountService uploaderAccountService
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.browserFactory = browserFactory;
        this.uploaderAccountService = uploaderAccountService;
        ensureSchema();
    }

    public List<ShipinhaoAccountStatus> accounts() {
        return repository.query(
                """
                SELECT ua.account_key, ua.last_upload_at, ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds, ua.upload_cooldown_max_seconds,
                       ua.today_upload_count, ua.cooldown_waiting_count, ua.upload_running_count,
                       ua.is_enabled,
                       pa.user_id, pa.nickname, pa.storage_state_json, pa.updated_at, pa.display_name, pa.avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_shipinhao pa ON pa.account_key = ua.account_key
                WHERE ua.platform = 'shipinhao'
                ORDER BY ua.account_key
                """,
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new ShipinhaoAccountStatus(
                            "database",
                            accountKey,
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

    public ShipinhaoAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        AccountSendAvailability availability = sendAvailability(normalized);
        int[] cooldown = cooldownConfig(normalized);
        if (storageState.isEmpty()) {
            return new ShipinhaoAccountStatus("database", normalized, false, 0, null, null, null, availability.lastUploadAt(), availability.nextUploadAllowedAt(), cooldown[0], cooldown[1], availability.todayUploadCount(), availability.cooldownWaitingCount(), availability.uploadRunningCount(), accountEnabled(normalized), false, "未登录", Map.of());
        }
        boolean valid = isStorageStateValid(storageState.get());
        AccountProfile profile = loadProfile(normalized);
        return new ShipinhaoAccountStatus(
                "database",
                normalized,
                true,
                storageState.get().getBytes(StandardCharsets.UTF_8).length,
                accountUpdatedAt(normalized).orElse(null),
                profile.userId(),
                profile.nickname(),
                availability.lastUploadAt(),
                availability.nextUploadAllowedAt(),
                cooldown[0],
                cooldown[1],
                availability.todayUploadCount(),
                availability.cooldownWaitingCount(),
                availability.uploadRunningCount(),
                accountEnabled(normalized),
                valid,
                valid ? "已登录" : "cookie 已失效",
                Map.of()
        );
    }

    public String storageState(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        return loadStorageState(normalized)
                .orElseThrow(() -> new IOException("Shipinhao account is not logged in: " + normalized));
    }

    public ShipinhaoAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
        String oldKey = normalizeAccountKey(oldAccountKey);
        String newKey = normalizeAccountKey(newAccountKey);
        if (oldKey.equals(newKey)) {
            return status(oldKey);
        }
        Integer exists = repository.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?", Integer.class, newKey);
        if (exists != null && exists > 0) {
            throw new IOException("Shipinhao account key already exists: " + newKey);
        }
        int updated = repository.update("UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?", newKey, oldKey);
        if (updated != 1) {
            throw new IOException("Shipinhao account key not found: " + oldKey);
        }
        uploaderAccountService.renameAccount("shipinhao", oldKey, newKey);
        return status(newKey);
    }

    public ShipinhaoAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Shipinhao account key not found: " + normalized);
        }
        uploaderAccountService.updateEnabled("shipinhao", normalized, enabled);
        return status(normalized);
    }

    public ShipinhaoAccountStatus setCooldown(String accountKey, Integer minSeconds, Integer maxSeconds) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int[] cooldown = normalizeCooldown(minSeconds, maxSeconds);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Shipinhao account key not found: " + normalized);
        }
        uploaderAccountService.updateCooldown("shipinhao", normalized, cooldown[0], cooldown[1]);
        return status(normalized);
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Shipinhao accountKey: " + accountKey);
        }
        return normalized;
    }

    Browser launchBrowser() {
        return browserFactory.launchBrowser(SocialBrowserPlatform.SHIPINHAO);
    }

    BrowserContext newContext(Browser browser, String storageState) {
        return browserFactory.newContext(SocialBrowserPlatform.SHIPINHAO, browser, storageState);
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        AccountProfile profile = profileFromStorageState(storageState);
        repository.update(
                """
                INSERT INTO uploader_account_shipinhao (account_key, user_id, nickname, storage_state_json, is_available, updated_at)
                VALUES (?, ?, ?, ?, 1, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), is_available = 1, updated_at = NOW()
                """,
                normalized,
                firstText(profile.userId(), loadProfile(normalized).userId()),
                firstText(profile.nickname(), loadProfile(normalized).nickname()),
                storageState
        );
        syncAccountState(normalized, null, null, null, null, null, LocalDateTime.now());
        uploaderAccountService.updateAvailable("shipinhao", normalized, true);
    }

    void markUnavailable(String accountKey) {
        String normalized = normalizeAccountKey(accountKey);
        repository.update(
                "UPDATE " + TABLE + " SET is_available = 0, updated_at = NOW() WHERE account_key = ?",
                normalized
        );
        uploaderAccountService.updateAvailable("shipinhao", normalized, false);
    }

    private boolean isStorageStateValid(String storageState) {
        try (Browser browser = launchBrowser()) {
            BrowserContext context = newContext(browser, storageState);
            try {
                Page page = context.newPage();
                page.navigate(HOME_URL);
                page.waitForTimeout(3000);
                String body = PlaywrightDiagnostics.safeBodyText(page);
                return !containsAny(body, "扫码登录", "微信扫码登录") && containsAny(body, "视频号", "首页", "内容管理");
            } finally {
                context.close();
            }
        } catch (Exception ignored) {
            return false;
        }
    }

    private Optional<String> loadStorageState(String accountKey) {
        List<String> values = repository.query(
                "SELECT storage_state_json FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getString("storage_state_json"),
                accountKey
        );
        if (values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            return Optional.empty();
        }
        return Optional.of(values.get(0));
    }

    private Optional<LocalDateTime> accountUpdatedAt(String accountKey) {
        List<LocalDateTime> values = repository.query(
                "SELECT updated_at FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getTimestamp("updated_at").toLocalDateTime(),
                accountKey
        );
        return values.stream().findFirst();
    }

    private AccountProfile loadProfile(String accountKey) {
        List<AccountProfile> values = repository.query(
                "SELECT user_id, nickname FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> new AccountProfile(rs.getString("user_id"), rs.getString("nickname")),
                accountKey
        );
        return values.stream().findFirst().orElse(new AccountProfile(null, null));
    }

    private AccountProfile profileFromStorageState(String storageState) throws IOException {
        JsonNode root = objectMapper.readTree(storageState);
        String userId = "";
        String nickname = "";
        for (JsonNode cookie : root.path("cookies")) {
            String name = cookie.path("name").asText("");
            if (name.toLowerCase().contains("uin") || name.toLowerCase().contains("user")) {
                userId = firstText(userId, cookie.path("value").asText(""));
            }
        }
        for (JsonNode origin : root.path("origins")) {
            for (JsonNode item : origin.path("localStorage")) {
                String name = item.path("name").asText("");
                String value = item.path("value").asText("");
                if ((name.toLowerCase().contains("user") || value.contains("nickname") || value.contains("finder")) && !value.isBlank()) {
                    try {
                        JsonNode parsed = objectMapper.readTree(value);
                        userId = firstText(userId, parsed.path("userId").asText(""), parsed.path("user_id").asText(""), parsed.path("id").asText(""));
                        nickname = firstText(nickname, parsed.path("nickname").asText(""), parsed.path("nickName").asText(""), parsed.path("name").asText(""));
                    } catch (Exception ignored) {
                        // Storage shape is best effort only.
                    }
                }
            }
        }
        return new AccountProfile(blankToNull(userId), blankToNull(nickname));
    }

    private int[] cooldownConfig(String accountKey) {
        UploaderAccountState state = uploaderAccountService.state("shipinhao", accountKey)
                .orElseGet(() -> UploaderAccountState.defaults("shipinhao", accountKey));
        return new int[] {
                state.uploadCooldownMinSeconds() == null ? 3600 : state.uploadCooldownMinSeconds(),
                state.uploadCooldownMaxSeconds() == null ? 7200 : state.uploadCooldownMaxSeconds()
        };
    }

    private int[] normalizeCooldown(Integer minSeconds, Integer maxSeconds) {
        int min = minSeconds == null ? 3600 : minSeconds;
        int max = maxSeconds == null ? 7200 : maxSeconds;
        if (min < 0 || max < 0 || min > max || max > 7 * 24 * 60 * 60) {
            throw new IllegalArgumentException("Invalid cooldown seconds range: " + min + "-" + max);
        }
        return new int[] {min, max};
    }

    private boolean accountEnabled(String accountKey) {
        return uploaderAccountService.state("shipinhao", accountKey)
                .map(UploaderAccountState::enabled)
                .orElse(true);
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("shipinhao", accountKey, TABLE);
    }

    private UploaderAccountState syncAccountState(
            String accountKey,
            Boolean enabled,
            Integer minSeconds,
            Integer maxSeconds,
            LocalDateTime lastUploadAt,
            LocalDateTime nextUploadAllowedAt,
            LocalDateTime sourceUpdatedAt
    ) {
        return uploaderAccountService.syncFromPlatformRow(
                "shipinhao",
                accountKey,
                TABLE,
                enabled,
                minSeconds,
                maxSeconds,
                lastUploadAt,
                nextUploadAllowedAt,
                sourceUpdatedAt
        );
    }

    private static Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private boolean accountKeyExists(String accountKey) {
        Integer exists = repository.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?", Integer.class, accountKey);
        return exists != null && exists > 0;
    }

    private void ensureSchema() {
        repository.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_account_shipinhao (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    account_key VARCHAR(64) NOT NULL,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_account_shipinhao_account_key (account_key)
                )
                """
        );
        AccountTableSchemaSupport.ensureSurrogatePrimaryKey(repository, TABLE);
        ensureColumn("display_name", "VARCHAR(128) NULL");
        ensureColumn("avatar_url", "VARCHAR(1024) NULL");
        ensureColumn("is_available", "TINYINT(1) NOT NULL DEFAULT 1");
    }

    private void ensureColumn(String column, String definition) {
        Integer count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                TABLE,
                column
        );
        if (count == null || count == 0) {
            repository.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + column + " " + definition);
        }
    }

    private boolean containsAny(String text, String... values) {
        return TextSupport.containsAny(text, values);
    }

    private String firstText(String... values) {
        return TextSupport.firstText(values);
    }

    private String blankToNull(String value) {
        String text = TextSupport.text(value);
        return text.isBlank() ? null : text;
    }

    private record AccountProfile(String userId, String nickname) {
    }
}
