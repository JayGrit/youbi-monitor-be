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
public class JinritoutiaoAccountService {
    static final String DEFAULT_ACCOUNT_KEY = "default";
    static final String HOME_URL = "https://mp.toutiao.com/profile_v4/";
    static final String PUBLISH_VIDEO_URL = "https://mp.toutiao.com/profile_v4/xigua/upload-video";
    static final String MANAGE_URL = "https://mp.toutiao.com/profile_v4/xigua/content-manage";

    private static final String TABLE = "uploader_account_jinritoutiao";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final SocialBrowserFactory browserFactory;

    public JinritoutiaoAccountService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            SocialBrowserFactory browserFactory
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.browserFactory = browserFactory;
        ensureSchema();
    }

    public List<JinritoutiaoAccountStatus> accounts() {
        return jdbcTemplate.query(
                "SELECT account_key, user_id, nickname, storage_state_json, updated_at, is_enabled, upload_cooldown_min_seconds, upload_cooldown_max_seconds, display_name, avatar_url FROM " + TABLE + " ORDER BY account_key",
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    AccountSendAvailability availability = sendAvailability(accountKey);
                    return new JinritoutiaoAccountStatus(
                            "database",
                            accountKey,
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime(),
                            rs.getString("user_id"),
                            rs.getString("nickname"),
                            availability.lastUploadAt(),
                            availability.nextUploadAllowedAt(),
                            nullableInt(rs, "upload_cooldown_min_seconds"),
                            nullableInt(rs, "upload_cooldown_max_seconds"),
                            availability.todayUploadCount(),
                            availability.cooldownWaitingCount(),
                            availability.uploadRunningCount(),
                            rs.getBoolean("is_enabled"),
                            null,
                            "已保存",
                            Map.of(),
                            rs.getString("display_name"),
                            rs.getString("avatar_url")
                    );
                }
        );
    }

    public JinritoutiaoAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        AccountSendAvailability availability = sendAvailability(normalized);
        int[] cooldown = cooldownConfig(normalized);
        if (storageState.isEmpty()) {
            return new JinritoutiaoAccountStatus("database", normalized, false, 0, null, null, null, availability.lastUploadAt(), availability.nextUploadAllowedAt(), cooldown[0], cooldown[1], availability.todayUploadCount(), availability.cooldownWaitingCount(), availability.uploadRunningCount(), accountEnabled(normalized), false, "未登录", Map.of());
        }
        boolean valid = isStorageStateValid(storageState.get());
        AccountProfile profile = loadProfile(normalized);
        return new JinritoutiaoAccountStatus(
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
                .orElseThrow(() -> new IOException("Jinritoutiao account is not logged in: " + normalized));
    }

    public JinritoutiaoAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
        String oldKey = normalizeAccountKey(oldAccountKey);
        String newKey = normalizeAccountKey(newAccountKey);
        if (oldKey.equals(newKey)) {
            return status(oldKey);
        }
        Integer exists = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?", Integer.class, newKey);
        if (exists != null && exists > 0) {
            throw new IOException("Jinritoutiao account key already exists: " + newKey);
        }
        int updated = jdbcTemplate.update("UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?", newKey, oldKey);
        if (updated != 1) {
            throw new IOException("Jinritoutiao account key not found: " + oldKey);
        }
        return status(newKey);
    }

    public JinritoutiaoAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int updated = jdbcTemplate.update("UPDATE " + TABLE + " SET is_enabled = ?, updated_at = NOW() WHERE account_key = ?", enabled, normalized);
        if (updated != 1) {
            throw new IOException("Jinritoutiao account key not found: " + normalized);
        }
        return status(normalized);
    }

    public JinritoutiaoAccountStatus setCooldown(String accountKey, Integer minSeconds, Integer maxSeconds) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int[] cooldown = normalizeCooldown(minSeconds, maxSeconds);
        int updated = jdbcTemplate.update(
                "UPDATE " + TABLE + " SET upload_cooldown_min_seconds = ?, upload_cooldown_max_seconds = ?, updated_at = NOW() WHERE account_key = ?",
                cooldown[0],
                cooldown[1],
                normalized
        );
        if (updated != 1) {
            throw new IOException("Jinritoutiao account key not found: " + normalized);
        }
        return status(normalized);
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Jinritoutiao accountKey: " + accountKey);
        }
        return normalized;
    }

    Browser launchBrowser() {
        return browserFactory.launchBrowser(SocialBrowserPlatform.JINRITOUTIAO);
    }

    BrowserContext newContext(Browser browser, String storageState) {
        return browserFactory.newContext(SocialBrowserPlatform.JINRITOUTIAO, browser, storageState);
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        AccountProfile profile = profileFromStorageState(storageState);
        jdbcTemplate.update(
                """
                INSERT INTO uploader_account_jinritoutiao (account_key, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                normalized,
                firstText(profile.userId(), loadProfile(normalized).userId()),
                firstText(profile.nickname(), loadProfile(normalized).nickname()),
                storageState
        );
    }

    private boolean isStorageStateValid(String storageState) {
        try (Browser browser = launchBrowser()) {
            BrowserContext context = newContext(browser, storageState);
            try {
                Page page = context.newPage();
                page.navigate(HOME_URL);
                page.waitForTimeout(3000);
                String body = PlaywrightDiagnostics.safeBodyText(page);
                return !containsAny(body, "扫码登录", "登录/注册", "手机登录", "验证码登录")
                        && containsAny(body, "头条号", "创作者", "作品管理", "发布视频", "消息");
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
        List<int[]> rows = jdbcTemplate.query(
                "SELECT upload_cooldown_min_seconds, upload_cooldown_max_seconds FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> new int[] {
                        rs.getObject("upload_cooldown_min_seconds") == null ? 3600 : rs.getInt("upload_cooldown_min_seconds"),
                        rs.getObject("upload_cooldown_max_seconds") == null ? 7200 : rs.getInt("upload_cooldown_max_seconds")
                },
                accountKey
        );
        return rows.isEmpty() ? new int[] {3600, 7200} : rows.get(0);
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
        List<Boolean> values = jdbcTemplate.query(
                "SELECT is_enabled FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getBoolean("is_enabled"),
                accountKey
        );
        return values.isEmpty() || values.get(0);
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("jinritoutiao", accountKey, TABLE);
    }

    private void ensureSchema() {
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_account_jinritoutiao (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                    upload_cooldown_min_seconds INT NOT NULL DEFAULT 3600,
                    upload_cooldown_max_seconds INT NOT NULL DEFAULT 7200,
                    last_upload_at DATETIME NULL,
                    next_upload_allowed_at DATETIME NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
        );
        ensureColumn("is_enabled", "TINYINT(1) NOT NULL DEFAULT 1");
        ensureColumn("upload_cooldown_min_seconds", "INT NOT NULL DEFAULT 3600");
        ensureColumn("upload_cooldown_max_seconds", "INT NOT NULL DEFAULT 7200");
        ensureColumn("last_upload_at", "DATETIME NULL");
        ensureColumn("next_upload_allowed_at", "DATETIME NULL");
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

    private Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
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
