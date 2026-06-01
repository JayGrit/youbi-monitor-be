package com.youbi.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class KuaishouAccountService {
    static final String DEFAULT_ACCOUNT_KEY = "default";
    static final String HOME_URL = "https://cp.kuaishou.com/profile";
    static final String PUBLISH_VIDEO_URL = "https://cp.kuaishou.com/article/publish/video";
    static final String MANAGE_URL = "https://cp.kuaishou.com/article/manage/video";

    private static final String TABLE = "uploader_account_kuaishou";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final SocialBrowserFactory browserFactory;
    private final UploaderAccountService uploaderAccountService;

    public KuaishouAccountService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            SocialBrowserFactory browserFactory,
            UploaderAccountService uploaderAccountService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.browserFactory = browserFactory;
        this.uploaderAccountService = uploaderAccountService;
        ensureSchema();
    }

    public List<KuaishouAccountStatus> accounts() {
        return jdbcTemplate.query(
                "SELECT account_key, user_id, nickname, storage_state_json, updated_at, display_name, avatar_url FROM " + TABLE + " ORDER BY account_key",
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    UploaderAccountState accountState = syncAccountState(
                            accountKey,
                            null,
                            null,
                            null,
                            null,
                            null,
                            updatedAt
                    );
                    return new KuaishouAccountStatus(
                            "database",
                            accountKey,
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            updatedAt,
                            rs.getString("user_id"),
                            rs.getString("nickname"),
                            accountState.lastUploadAt(),
                            accountState.nextUploadAllowedAt(),
                            accountState.uploadCooldownMinSeconds(),
                            accountState.uploadCooldownMaxSeconds(),
                            accountState.todayUploadCount(),
                            accountState.cooldownWaitingCount(),
                            accountState.uploadRunningCount(),
                            accountState.enabled(),
                            null,
                            "已保存",
                            Map.of(),
                            rs.getString("display_name"),
                            rs.getString("avatar_url")
                    );
                }
        );
    }

    public KuaishouAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        AccountSendAvailability availability = sendAvailability(normalized);
        int[] cooldown = cooldownConfig(normalized);
        if (storageState.isEmpty()) {
            return new KuaishouAccountStatus("database", normalized, false, 0, null, null, null, availability.lastUploadAt(), availability.nextUploadAllowedAt(), cooldown[0], cooldown[1], availability.todayUploadCount(), availability.cooldownWaitingCount(), availability.uploadRunningCount(), accountEnabled(normalized), false, "未登录", Map.of());
        }
        boolean valid = isStorageStateValid(storageState.get());
        AccountProfile profile = loadProfile(normalized);
        return new KuaishouAccountStatus(
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
                .orElseThrow(() -> new IOException("Kuaishou account is not logged in: " + normalized));
    }

    public KuaishouAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
        String oldKey = normalizeAccountKey(oldAccountKey);
        String newKey = normalizeAccountKey(newAccountKey);
        if (oldKey.equals(newKey)) {
            return status(oldKey);
        }
        Integer exists = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?", Integer.class, newKey);
        if (exists != null && exists > 0) {
            throw new IOException("Kuaishou account key already exists: " + newKey);
        }
        int updated = jdbcTemplate.update("UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?", newKey, oldKey);
        if (updated != 1) {
            throw new IOException("Kuaishou account key not found: " + oldKey);
        }
        uploaderAccountService.renameAccount("kuaishou", oldKey, newKey);
        return status(newKey);
    }

    public KuaishouAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Kuaishou account key not found: " + normalized);
        }
        uploaderAccountService.updateEnabled("kuaishou", normalized, enabled);
        return status(normalized);
    }

    public KuaishouAccountStatus setCooldown(String accountKey, Integer minSeconds, Integer maxSeconds) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int[] cooldown = normalizeCooldown(minSeconds, maxSeconds);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Kuaishou account key not found: " + normalized);
        }
        uploaderAccountService.updateCooldown("kuaishou", normalized, cooldown[0], cooldown[1]);
        return status(normalized);
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Kuaishou accountKey: " + accountKey);
        }
        return normalized;
    }

    Browser launchBrowser() {
        return browserFactory.launchBrowser(SocialBrowserPlatform.KUAISHOU);
    }

    BrowserContext newContext(Browser browser, String storageState) {
        return browserFactory.newContext(SocialBrowserPlatform.KUAISHOU, browser, storageState);
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        AccountProfile profile = profileFromStorageState(storageState);
        jdbcTemplate.update(
                """
                INSERT INTO uploader_account_kuaishou (account_key, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                normalized,
                firstText(profile.userId(), loadProfile(normalized).userId()),
                firstText(profile.nickname(), loadProfile(normalized).nickname()),
                storageState
        );
        syncAccountState(normalized, null, null, null, null, null, LocalDateTime.now());
    }

    private boolean isStorageStateValid(String storageState) {
        try (Browser browser = launchBrowser()) {
            BrowserContext context = newContext(browser, storageState);
            try {
                Page page = context.newPage();
                page.navigate(HOME_URL);
                page.waitForTimeout(3000);
                String body = PlaywrightDiagnostics.safeBodyText(page);
                return !containsAny(body, "扫码登录", "登录/注册", "请使用快手") && containsAny(body, "快手", "创作者", "作品管理", "发布视频");
            } finally {
                context.close();
            }
        } catch (Exception ignored) {
            return false;
        }
    }

    private Optional<String> loadStorageState(String accountKey) {
        List<String> values = jdbcTemplate.query(
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
        List<LocalDateTime> values = jdbcTemplate.query(
                "SELECT updated_at FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getTimestamp("updated_at").toLocalDateTime(),
                accountKey
        );
        return values.stream().findFirst();
    }

    private AccountProfile loadProfile(String accountKey) {
        List<AccountProfile> values = jdbcTemplate.query(
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
            if (name.toLowerCase().contains("uid") || name.toLowerCase().contains("user")) {
                userId = firstText(userId, cookie.path("value").asText(""));
            }
        }
        for (JsonNode origin : root.path("origins")) {
            for (JsonNode item : origin.path("localStorage")) {
                String name = item.path("name").asText("");
                String value = item.path("value").asText("");
                if ((name.toLowerCase().contains("user") || value.contains("nickname") || value.contains("userName")) && !value.isBlank()) {
                    try {
                        JsonNode parsed = objectMapper.readTree(value);
                        userId = firstText(userId, parsed.path("userId").asText(""), parsed.path("user_id").asText(""), parsed.path("uid").asText(""), parsed.path("id").asText(""));
                        nickname = firstText(nickname, parsed.path("nickname").asText(""), parsed.path("nickName").asText(""), parsed.path("userName").asText(""), parsed.path("name").asText(""));
                    } catch (Exception ignored) {
                        // Storage shape is best effort only.
                    }
                }
            }
        }
        return new AccountProfile(blankToNull(userId), blankToNull(nickname));
    }

    private int[] cooldownConfig(String accountKey) {
        UploaderAccountState state = uploaderAccountService.state("kuaishou", accountKey)
                .orElseGet(() -> UploaderAccountState.defaults("kuaishou", accountKey));
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
        return uploaderAccountService.state("kuaishou", accountKey)
                .map(UploaderAccountState::enabled)
                .orElse(true);
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("kuaishou", accountKey, TABLE);
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
                "kuaishou",
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

    private boolean accountKeyExists(String accountKey) {
        Integer exists = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?", Integer.class, accountKey);
        return exists != null && exists > 0;
    }

    private void ensureSchema() {
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_account_kuaishou (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
        );
        ensureColumn("display_name", "VARCHAR(128) NULL");
        ensureColumn("avatar_url", "VARCHAR(1024) NULL");
    }

    private void ensureColumn(String column, String definition) {
        Integer count = jdbcTemplate.queryForObject(
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
            jdbcTemplate.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + column + " " + definition);
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
