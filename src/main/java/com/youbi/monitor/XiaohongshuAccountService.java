package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.TimeoutError;
import org.springframework.beans.factory.annotation.Value;
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

    private static final String TABLE = "yd_xiaohongshu_account";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final boolean headless;
    private final String browserChannel;
    private final Map<String, LoginSession> loginSessions = new ConcurrentHashMap<>();

    public XiaohongshuAccountService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            @Value("${youbi.xiaohongshu.headless:true}") boolean headless,
            @Value("${youbi.xiaohongshu.browser-channel:chrome}") String browserChannel
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.headless = headless;
        this.browserChannel = browserChannel == null || browserChannel.isBlank() ? "chrome" : browserChannel.trim();
        ensureSchema();
    }

    public List<XiaohongshuAccountStatus> accounts() {
        return jdbcTemplate.query(
                "SELECT account_key, user_id, nickname, storage_state_json, updated_at, is_enabled FROM " + TABLE + " ORDER BY account_key",
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("storage_state_json");
                    AccountSendAvailability sendAvailability = sendAvailability(accountKey);
                    return new XiaohongshuAccountStatus(
                            "database",
                            accountKey,
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime(),
                            rs.getString("user_id"),
                            rs.getString("nickname"),
                            sendAvailability.lastUploadAt(),
                            sendAvailability.nextUploadAllowedAt(),
                            sendAvailability.todayUploadCount(),
                            sendAvailability.cooldownWaitingCount(),
                            rs.getBoolean("is_enabled"),
                            null,
                            "已保存",
                            Map.of()
                    );
                }
        );
    }

    public XiaohongshuAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        AccountSendAvailability sendAvailability = sendAvailability(normalized);
        if (storageState.isEmpty()) {
            return new XiaohongshuAccountStatus("database", normalized, false, 0, null, null, null, sendAvailability.lastUploadAt(), sendAvailability.nextUploadAllowedAt(), sendAvailability.todayUploadCount(), sendAvailability.cooldownWaitingCount(), accountEnabled(normalized), false, "未登录", Map.of());
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
                sendAvailability.todayUploadCount(),
                sendAvailability.cooldownWaitingCount(),
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

        try {
            Playwright playwright = Playwright.create();
            Browser browser = playwright.chromium().launch(new BrowserTypeOptions(headless, browserChannel).toLaunchOptions());
            BrowserContext context = browser.newContext();
            Page page = context.newPage();
            page.navigate(LOGIN_URL);
            String imageDataUrl = extractQrImage(page);
            loginSessions.put(authCode, new LoginSession(normalized, authCode, playwright, browser, context, page, Instant.now().plusSeconds(180)));
            return new XiaohongshuQrCode(normalized, authCode, imageDataUrl, Instant.now().getEpochSecond() + 180);
        } catch (Exception exception) {
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
        return status(newKey);
    }

    public XiaohongshuAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int updated = jdbcTemplate.update(
                "UPDATE " + TABLE + " SET is_enabled = ?, updated_at = NOW() WHERE account_key = ?",
                enabled,
                normalized
        );
        if (updated != 1) {
            throw new IOException("Xiaohongshu account key not found: " + normalized);
        }
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
        return PlaywrightHolder.playwright().chromium().launch(new BrowserTypeOptions(headless, browserChannel).toLaunchOptions());
    }

    Browser.NewContextOptions storageContextOptions(String storageState) {
        return new Browser.NewContextOptions().setStorageState(storageState);
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        AccountProfile profile = profileFromStorageState(storageState);
        jdbcTemplate.update(
                """
                INSERT INTO yd_xiaohongshu_account (account_key, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                normalized,
                profile.userId(),
                profile.nickname(),
                storageState
        );
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
        return new XiaohongshuAccountStatus("database", accountKey, false, 0, null, null, null, null, null, 0, 0, true, false, "等待扫码", Map.of());
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("xiaohongshu", accountKey, TABLE);
    }

    private boolean accountEnabled(String accountKey) {
        List<Boolean> values = jdbcTemplate.query(
                "SELECT is_enabled FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getBoolean("is_enabled"),
                accountKey
        );
        return values.isEmpty() || values.get(0);
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
            BrowserContext context = browser.newContext(storageContextOptions(storageState));
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
                CREATE TABLE IF NOT EXISTS yd_xiaohongshu_account (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
        );
        ensureColumn("is_enabled", "TINYINT(1) NOT NULL DEFAULT 1");
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
        try {
            session.context().close();
        } catch (Exception ignored) {
        }
        try {
            session.browser().close();
        } catch (Exception ignored) {
        }
        try {
            session.playwright().close();
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

    private record LoginSession(String accountKey, String authCode, Playwright playwright, Browser browser,
                                BrowserContext context, Page page, Instant expiresAt) {
    }

    private record AccountProfile(String userId, String nickname) {
    }

    private record BrowserTypeOptions(boolean headless, String channel) {
        com.microsoft.playwright.BrowserType.LaunchOptions toLaunchOptions() {
            return new com.microsoft.playwright.BrowserType.LaunchOptions()
                    .setHeadless(headless)
                    .setChannel(channel)
                    .setArgs(List.of("--no-sandbox", "--disable-dev-shm-usage"));
        }
    }

    private static class PlaywrightHolder {
        private static final Playwright PLAYWRIGHT = Playwright.create();

        static Playwright playwright() {
            return PLAYWRIGHT;
        }
    }
}
