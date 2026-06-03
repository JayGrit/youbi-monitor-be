package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.TimeoutError;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class XiaohongshuAccountService {
    static final String DEFAULT_ACCOUNT_KEY = "default";
    static final String AUTO_ACCOUNT_KEY = "_auto";
    static final String LOGIN_URL = "https://creator.xiaohongshu.com/login";
    static final String PUBLISH_VIDEO_URL = "https://creator.xiaohongshu.com/publish/publish?from=homepage&target=video";
    static final String LOGIN_BOX_SELECTOR = "div[class*='login-box']";

    private static final String TABLE = "uploader_account_xiaohongshu";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final SocialBrowserFactory browserFactory;
    private final UploaderAccountService uploaderAccountService;
    private final Map<String, LoginSession> loginSessions = new ConcurrentHashMap<>();

    public XiaohongshuAccountService(
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

    public List<XiaohongshuAccountStatus> accounts() {
        return jdbcTemplate.query(
                """
                SELECT ua.account_key, ua.last_upload_at, ua.next_upload_allowed_at,
                       ua.upload_cooldown_min_seconds, ua.upload_cooldown_max_seconds,
                       ua.today_upload_count, ua.cooldown_waiting_count, ua.upload_running_count,
                       ua.is_enabled,
                       pa.user_id, pa.nickname, pa.storage_state_json, pa.updated_at, pa.display_name, pa.avatar_url
                FROM uploader_account ua
                LEFT JOIN uploader_account_xiaohongshu pa ON pa.account_key = ua.account_key
                WHERE ua.platform = 'xiaohongshu'
                ORDER BY ua.account_key
                """,
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new XiaohongshuAccountStatus(
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

    public XiaohongshuAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        AccountSendAvailability sendAvailability = sendAvailability(normalized);
        int[] cooldown = cooldownConfig(normalized);
        if (storageState.isEmpty()) {
            return new XiaohongshuAccountStatus("database", normalized, false, 0, null, null, null, sendAvailability.lastUploadAt(), sendAvailability.nextUploadAllowedAt(), cooldown[0], cooldown[1], sendAvailability.todayUploadCount(), sendAvailability.cooldownWaitingCount(), sendAvailability.uploadRunningCount(), accountEnabled(normalized), false, "未登录", Map.of());
        }
        boolean valid = isStorageStateValid(storageState.get());
        LocalDateTime updatedAt = accountUpdatedAt(normalized).orElse(null);
        AccountProfile profile = loadProfile(normalized);
        return new XiaohongshuAccountStatus(
                "database",
                normalized,
                true,
                storageState.get().getBytes(StandardCharsets.UTF_8).length,
                updatedAt,
                profile.userId(),
                profile.nickname(),
                sendAvailability.lastUploadAt(),
                sendAvailability.nextUploadAllowedAt(),
                cooldown[0],
                cooldown[1],
                sendAvailability.todayUploadCount(),
                sendAvailability.cooldownWaitingCount(),
                sendAvailability.uploadRunningCount(),
                accountEnabled(normalized),
                valid,
                valid ? "已登录" : "cookie 已失效",
                Map.of()
        );
    }

    public String storageState(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        return loadStorageState(normalized)
                .orElseThrow(() -> new IOException("Xiaohongshu account is not logged in: " + normalized));
    }

    public XiaohongshuQrCode createQrCode(String accountKey) throws IOException {
        String normalized = normalizeRequestedAccountKey(accountKey);
        String authCode = UUID.randomUUID().toString();
        closeSession(authCode);

        Browser browser = null;
        BrowserContext context = null;
        try {
            browser = launchBrowser();
            context = browserFactory.newContext(SocialBrowserPlatform.XIAOHONGSHU, browser);
            Page page = context.newPage();
            page.navigate(LOGIN_URL);
            String imageDataUrl = extractQrImage(page);
            loginSessions.put(authCode, new LoginSession(normalized, authCode, browser, context, page, Instant.now().plusSeconds(180)));
            return new XiaohongshuQrCode(normalized, authCode, imageDataUrl, Instant.now().getEpochSecond() + 180);
        } catch (Exception exception) {
            closeQuietly(context, browser);
            throw new IOException("Cannot create Xiaohongshu qrcode: " + exception.getMessage(), exception);
        }
    }

    public XiaohongshuQrPollResult pollQrCode(String accountKey, String authCode) throws IOException {
        LoginSession session = loginSessions.get(authCode);
        if (session == null) {
            return new XiaohongshuQrPollResult(false, "missing", "二维码会话不存在或已过期", emptyStatus(normalizeRequestedAccountKey(accountKey)));
        }
        if (Instant.now().isAfter(session.expiresAt())) {
            closeSession(authCode);
            return new XiaohongshuQrPollResult(false, "expired", "二维码已过期", emptyStatus(session.accountKey()));
        }

        try {
            if (!isLoginCompleted(session.page())) {
                return new XiaohongshuQrPollResult(false, "waiting", "等待扫码确认", emptyStatus(session.accountKey()));
            }

            session.page().waitForTimeout(2000);
            String storageState = session.context().storageState();
            String saveKey = AUTO_ACCOUNT_KEY.equals(session.accountKey()) ? automaticAccountKey(storageState) : session.accountKey();
            saveStorageState(saveKey, storageState);
            XiaohongshuAccountStatus status = status(saveKey);
            closeSession(authCode);
            return new XiaohongshuQrPollResult(true, "success", "登录成功", status);
        } catch (Exception exception) {
            throw new IOException("Cannot poll Xiaohongshu qrcode: " + exception.getMessage(), exception);
        }
    }

    public XiaohongshuAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
        String oldKey = normalizeAccountKey(oldAccountKey);
        String newKey = normalizeAccountKey(newAccountKey);
        if (oldKey.equals(newKey)) {
            return status(oldKey);
        }
        Integer exists = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?",
                Integer.class,
                newKey
        );
        if (exists != null && exists > 0) {
            throw new IOException("Xiaohongshu account key already exists: " + newKey);
        }
        int updated = jdbcTemplate.update(
                "UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?",
                newKey,
                oldKey
        );
        if (updated != 1) {
            throw new IOException("Xiaohongshu account key not found: " + oldKey);
        }
        uploaderAccountService.renameAccount("xiaohongshu", oldKey, newKey);
        return status(newKey);
    }

    public XiaohongshuAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Xiaohongshu account key not found: " + normalized);
        }
        uploaderAccountService.updateEnabled("xiaohongshu", normalized, enabled);
        return status(normalized);
    }

    public XiaohongshuAccountStatus setCooldown(String accountKey, Integer minSeconds, Integer maxSeconds) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int[] cooldown = normalizeCooldown(minSeconds, maxSeconds);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Xiaohongshu account key not found: " + normalized);
        }
        uploaderAccountService.updateCooldown("xiaohongshu", normalized, cooldown[0], cooldown[1]);
        return status(normalized);
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Xiaohongshu accountKey: " + accountKey);
        }
        return normalized;
    }

    Browser launchBrowser() {
        return browserFactory.launchBrowser(SocialBrowserPlatform.XIAOHONGSHU);
    }

    Browser.NewContextOptions storageContextOptions(String storageState) {
        return browserFactory.storageContextOptions(SocialBrowserPlatform.XIAOHONGSHU, storageState);
    }

    BrowserContext newContext(Browser browser, String storageState) {
        return browserFactory.newContext(SocialBrowserPlatform.XIAOHONGSHU, browser, storageState);
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        AccountProfile profile = profileFromStorageState(storageState);
        jdbcTemplate.update(
                """
                INSERT INTO uploader_account_xiaohongshu (account_key, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                normalized,
                profile.userId(),
                profile.nickname(),
                storageState
        );
        syncAccountState(normalized, null, null, null, null, null, LocalDateTime.now());
    }

    private String normalizeRequestedAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return AUTO_ACCOUNT_KEY;
        }
        if (AUTO_ACCOUNT_KEY.equals(normalized)) {
            return normalized;
        }
        return normalizeAccountKey(normalized);
    }

    private XiaohongshuAccountStatus emptyStatus(String accountKey) {
        return new XiaohongshuAccountStatus("database", accountKey, false, 0, null, null, null, null, null, null, null, 0, 0, 0, true, false, "等待扫码", Map.of());
    }

    private int[] cooldownConfig(String accountKey) {
        UploaderAccountState state = uploaderAccountService.state("xiaohongshu", accountKey)
                .orElseGet(() -> UploaderAccountState.defaults("xiaohongshu", accountKey));
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

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("xiaohongshu", accountKey, TABLE);
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
                "xiaohongshu",
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

    private boolean accountEnabled(String accountKey) {
        return uploaderAccountService.state("xiaohongshu", accountKey)
                .map(UploaderAccountState::enabled)
                .orElse(true);
    }

    private static Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private String extractQrImage(Page page) {
        openQrPanel(page);
        Locator image = page.locator(".login-box-container").getByText("APP扫一扫登录").filter(new Locator.FilterOptions().setVisible(true))
                .locator("xpath=..//following-sibling::div//img").first();
        image.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        String src = image.getAttribute("src");
        if (src != null && !src.isBlank()) {
            return src;
        }
        byte[] screenshot = image.screenshot();
        return "data:image/png;base64," + Base64.getEncoder().encodeToString(screenshot);
    }

    private void openQrPanel(Page page) {
        Locator loginBox = page.locator(LOGIN_BOX_SELECTOR).first();
        loginBox.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        if (loginBox.locator("div:has-text('扫一扫')").first().count() > 0) {
            return;
        }
        Locator switchImg = loginBox.locator("img.css-wemwzq").first();
        switchImg.waitFor(new Locator.WaitForOptions().setTimeout(10000));
        switchImg.click();
        loginBox.locator("div:has-text('扫一扫')").first().waitFor(new Locator.WaitForOptions().setTimeout(10000));
    }

    private boolean isLoginCompleted(Page page) {
        if (page.url().startsWith(LOGIN_URL)) {
            return false;
        }
        Locator loginBox = page.locator(LOGIN_BOX_SELECTOR).first();
        if (loginBox.count() == 0) {
            return true;
        }
        try {
            return !loginBox.isVisible();
        } catch (Exception ignored) {
            return true;
        }
    }

    private boolean isStorageStateValid(String storageState) {
        try (Browser browser = launchBrowser()) {
            BrowserContext context = newContext(browser, storageState);
            try {
                Page page = context.newPage();
                page.navigate(PUBLISH_VIDEO_URL);
                page.waitForTimeout(3000);
                return isLoginCompleted(page);
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

    private String automaticAccountKey(String storageState) throws IOException {
        AccountProfile profile = profileFromStorageState(storageState);
        String userId = text(profile.userId());
        if (!userId.isBlank()) {
            List<String> existing = jdbcTemplate.query(
                    "SELECT account_key FROM " + TABLE + " WHERE user_id = ? ORDER BY updated_at DESC LIMIT 1",
                    (rs, rowNum) -> rs.getString("account_key"),
                    userId
            );
            if (!existing.isEmpty()) {
                return existing.get(0);
            }
        }
        String base = userId.isBlank() ? "account" : "uid_" + userId.replaceAll("[^A-Za-z0-9_.-]+", "_");
        String candidate = base;
        for (int index = 2; accountKeyExists(candidate); index++) {
            candidate = base + "_" + index;
        }
        return candidate;
    }

    private boolean accountKeyExists(String accountKey) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?",
                Integer.class,
                accountKey
        );
        return count != null && count > 0;
    }

    private AccountProfile profileFromStorageState(String storageState) throws IOException {
        JsonNode root = objectMapper.readTree(storageState);
        String userId = "";
        String nickname = "";
        for (JsonNode origin : root.path("origins")) {
            for (JsonNode item : origin.path("localStorage")) {
                String name = item.path("name").asText("");
                String value = item.path("value").asText("");
                if (name.toLowerCase().contains("user") && !value.isBlank()) {
                    try {
                        JsonNode parsed = objectMapper.readTree(value);
                        userId = firstText(userId, parsed.path("userId").asText(""), parsed.path("user_id").asText(""), parsed.path("id").asText(""));
                        nickname = firstText(nickname, parsed.path("nickname").asText(""), parsed.path("name").asText(""));
                    } catch (Exception ignored) {
                        // Local storage shape is not contractual; profile fields are best effort.
                    }
                }
            }
        }
        return new AccountProfile(blankToNull(userId), blankToNull(nickname));
    }

    private void ensureSchema() {
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS uploader_account_xiaohongshu (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    account_key VARCHAR(64) NOT NULL,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_uploader_account_xiaohongshu_account_key (account_key)
                )
                """
        );
        AccountTableSchemaSupport.ensureSurrogatePrimaryKey(jdbcTemplate, TABLE);
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

    private void closeSession(String authCode) {
        LoginSession session = loginSessions.remove(authCode);
        if (session == null) {
            return;
        }
        closeQuietly(session.context(), session.browser());
    }

    private void closeQuietly(BrowserContext context, Browser browser) {
        try {
            if (context != null) {
                context.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (browser != null) {
                browser.close();
            }
        } catch (Exception ignored) {
        }
    }

    private String firstText(String... values) {
        for (String value : values) {
            String text = text(value);
            if (!text.isBlank()) {
                return text;
            }
        }
        return "";
    }

    private String blankToNull(String value) {
        String text = text(value);
        return text.isBlank() ? null : text;
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record LoginSession(String accountKey, String authCode, Browser browser,
                                BrowserContext context, Page page, Instant expiresAt) {
    }

    private record AccountProfile(String userId, String nickname) {
    }

}
