package com.youbi.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class BilibiliPlaywrightAccountService {
    static final String DEFAULT_ACCOUNT_KEY = "knowledge";
    static final String PUBLISH_VIDEO_URL = "https://member.bilibili.com/platform/upload/video/frame?page_from=creative_home_top_upload";

    private static final String TABLE = "yd_bilibili_account";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final HttpClient httpClient;
    private final boolean headless;
    private final String browserChannel;
    private final Map<String, LoginSession> loginSessions = new ConcurrentHashMap<>();

    public BilibiliPlaywrightAccountService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            @Value("${youbi.bilibili.playwright.headless:false}") boolean headless,
            @Value("${youbi.bilibili.playwright.browser-channel:chrome}") String browserChannel
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).followRedirects(HttpClient.Redirect.NORMAL).build();
        this.headless = headless;
        this.browserChannel = browserChannel == null ? "" : browserChannel.trim();
        ensureSchema();
    }

    public BilibiliPlaywrightLoginOpenResult openManualLogin(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        String authCode = UUID.randomUUID().toString();
        closeSession(authCode);
        try {
            Playwright playwright = Playwright.create();
            Browser browser = playwright.chromium().launch(new BrowserTypeOptions(headless, browserChannel).toLaunchOptions());
            BrowserContext context = loadStorageState(normalized)
                    .map(state -> browser.newContext(storageContextOptions(state)))
                    .orElseGet(browser::newContext);
            Page page = context.newPage();
            page.navigate(PUBLISH_VIDEO_URL);
            loginSessions.put(authCode, new LoginSession(normalized, authCode, playwright, browser, context, page, Instant.now().plusSeconds(900)));
            return new BilibiliPlaywrightLoginOpenResult(normalized, authCode, page.url(), Instant.now().getEpochSecond() + 900);
        } catch (Exception exception) {
            throw new IOException("Cannot open Bilibili login page: " + exception.getMessage(), exception);
        }
    }

    public BilibiliPlaywrightLoginPollResult pollManualLogin(String accountKey, String authCode) throws IOException {
        LoginSession session = loginSessions.get(text(authCode));
        String normalized = normalizeAccountKey(accountKey);
        if (session == null) {
            return new BilibiliPlaywrightLoginPollResult(false, "missing", "登录窗口会话不存在或已过期", emptyStatus(normalized, "未登录"));
        }
        if (Instant.now().isAfter(session.expiresAt())) {
            closeSession(authCode);
            return new BilibiliPlaywrightLoginPollResult(false, "expired", "登录窗口已过期", emptyStatus(session.accountKey(), "未登录"));
        }

        String storageState = session.context().storageState();
        AccountProfile profile = profileFromCookieState(storageState).orElse(new AccountProfile(null, null));
        boolean loggedIn = profile.mid() != null || hasBilibiliLoginCookie(storageState);
        if (!loggedIn) {
            return new BilibiliPlaywrightLoginPollResult(false, "waiting", "等待手动登录", emptyStatus(session.accountKey(), "等待登录"));
        }

        saveStorageState(session.accountKey(), storageState, profile);
        BilibiliPlaywrightAccountStatus status = status(session.accountKey());
        closeSession(authCode);
        return new BilibiliPlaywrightLoginPollResult(true, "success", "登录成功", status);
    }

    public List<BilibiliPlaywrightAccountStatus> accounts() {
        return jdbcTemplate.query(
                """
                SELECT account_key, mid, uname, playwright_mid, playwright_uname, playwright_storage_state_json, playwright_updated_at
                FROM yd_bilibili_account
                ORDER BY account_key
                """,
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String storageState = rs.getString("playwright_storage_state_json");
                    AccountSendAvailability sendAvailability = sendAvailability(accountKey);
                    return new BilibiliPlaywrightAccountStatus(
                            "database",
                            accountKey,
                            storageState != null && !storageState.isBlank(),
                            storageState == null ? 0 : storageState.getBytes(StandardCharsets.UTF_8).length,
                            rs.getTimestamp("playwright_updated_at") == null ? null : rs.getTimestamp("playwright_updated_at").toLocalDateTime(),
                            rs.getObject("playwright_mid") == null ? (rs.getObject("mid") == null ? null : rs.getLong("mid")) : rs.getLong("playwright_mid"),
                            blankToNull(rs.getString("playwright_uname")) == null ? rs.getString("uname") : rs.getString("playwright_uname"),
                            sendAvailability.lastUploadAt(),
                            sendAvailability.nextUploadAllowedAt(),
                            null,
                            "已保存",
                            Map.of()
                    );
                }
        );
    }

    public BilibiliPlaywrightAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        if (storageState.isEmpty()) {
            return emptyStatus(normalized, "未登录");
        }
        AccountProfile profile = loadProfile(normalized);
        boolean valid = isStorageStateValid(storageState.get());
        AccountSendAvailability sendAvailability = sendAvailability(normalized);
        return new BilibiliPlaywrightAccountStatus(
                "database",
                normalized,
                true,
                storageState.get().getBytes(StandardCharsets.UTF_8).length,
                accountUpdatedAt(normalized).orElse(null),
                profile.mid(),
                profile.uname(),
                sendAvailability.lastUploadAt(),
                sendAvailability.nextUploadAllowedAt(),
                valid,
                valid ? "已登录" : "cookie 已失效",
                Map.of()
        );
    }

    public String storageState(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        return loadStorageState(normalized)
                .orElseThrow(() -> new IOException("Bilibili Playwright account is not logged in: " + normalized));
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = text(accountKey);
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Bilibili Playwright accountKey: " + accountKey);
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
        saveStorageState(normalizeAccountKey(accountKey), storageState, profileFromCookieState(storageState).orElse(new AccountProfile(null, null)));
    }

    private void saveStorageState(String accountKey, String storageState, AccountProfile profile) {
        int updated = jdbcTemplate.update(
                """
                UPDATE yd_bilibili_account
                SET playwright_mid = ?, playwright_uname = ?, playwright_storage_state_json = ?, playwright_updated_at = NOW()
                WHERE account_key = ?
                """,
                profile.mid(),
                profile.uname(),
                storageState,
                accountKey
        );
        if (updated == 0) {
            throw new IllegalArgumentException("Bilibili account key not found: " + accountKey);
        }
    }

    private boolean isStorageStateValid(String storageState) {
        try {
            Optional<AccountProfile> profile = profileFromCookieState(storageState);
            return profile.map(AccountProfile::mid).orElse(null) != null || hasBilibiliLoginCookie(storageState);
        } catch (Exception ignored) {
            return false;
        }
    }

    private Optional<AccountProfile> profileFromCookieState(String storageState) throws IOException {
        String cookie = cookieHeader(storageState);
        if (cookie.isBlank()) {
            return Optional.empty();
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://api.bilibili.com/x/space/myinfo"))
                .timeout(Duration.ofSeconds(15))
                .header("User-Agent", "Mozilla/5.0")
                .header("Referer", "https://www.bilibili.com/")
                .header("Cookie", cookie)
                .GET()
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() / 100 != 2) {
                return Optional.empty();
            }
            JsonNode root = objectMapper.readTree(response.body());
            if (root.path("code").asInt(Integer.MIN_VALUE) != 0) {
                return Optional.empty();
            }
            JsonNode data = root.path("data");
            Long mid = data.path("mid").canConvertToLong() ? data.path("mid").asLong() : null;
            String uname = data.path("name").asText("");
            return Optional.of(new AccountProfile(mid, blankToNull(uname)));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted checking Bilibili profile", exception);
        }
    }

    private String cookieHeader(String storageState) throws IOException {
        JsonNode root = objectMapper.readTree(storageState);
        StringBuilder builder = new StringBuilder();
        for (JsonNode cookie : root.path("cookies")) {
            String domain = cookie.path("domain").asText("");
            if (!domain.contains("bilibili.com")) {
                continue;
            }
            String name = cookie.path("name").asText("");
            String value = cookie.path("value").asText("");
            if (!name.isBlank() && !value.isBlank()) {
                if (!builder.isEmpty()) {
                    builder.append("; ");
                }
                builder.append(name).append("=").append(value);
            }
        }
        return builder.toString();
    }

    private boolean hasBilibiliLoginCookie(String storageState) throws IOException {
        JsonNode root = objectMapper.readTree(storageState);
        boolean hasDedeUserId = false;
        boolean hasSessdata = false;
        for (JsonNode cookie : root.path("cookies")) {
            String domain = cookie.path("domain").asText("");
            String name = cookie.path("name").asText("");
            String value = cookie.path("value").asText("");
            if (domain.contains("bilibili.com") && !value.isBlank()) {
                hasDedeUserId = hasDedeUserId || "DedeUserID".equals(name);
                hasSessdata = hasSessdata || "SESSDATA".equals(name);
            }
        }
        return hasDedeUserId && hasSessdata;
    }

    private Optional<String> loadStorageState(String accountKey) {
        List<String> values = jdbcTemplate.query(
                "SELECT playwright_storage_state_json FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getString("playwright_storage_state_json"),
                accountKey
        );
        if (values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            return Optional.empty();
        }
        return Optional.of(values.get(0));
    }

    private AccountProfile loadProfile(String accountKey) {
        List<AccountProfile> values = jdbcTemplate.query(
                "SELECT mid, uname, playwright_mid, playwright_uname FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> new AccountProfile(
                        rs.getObject("playwright_mid") == null ? (rs.getObject("mid") == null ? null : rs.getLong("mid")) : rs.getLong("playwright_mid"),
                        blankToNull(rs.getString("playwright_uname")) == null ? rs.getString("uname") : rs.getString("playwright_uname")
                ),
                accountKey
        );
        return values.stream().findFirst().orElse(new AccountProfile(null, null));
    }

    private Optional<LocalDateTime> accountUpdatedAt(String accountKey) {
        List<LocalDateTime> values = jdbcTemplate.query(
                "SELECT playwright_updated_at FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getTimestamp("playwright_updated_at") == null ? null : rs.getTimestamp("playwright_updated_at").toLocalDateTime(),
                accountKey
        );
        return values.stream().findFirst();
    }

    private BilibiliPlaywrightAccountStatus emptyStatus(String accountKey, String message) {
        return new BilibiliPlaywrightAccountStatus("database", accountKey, false, 0, null, null, null, null, null, false, message, Map.of());
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("bilibili", accountKey, "yd_bilibili_account");
    }

    private void ensureSchema() {
        ensureColumn("playwright_mid", "BIGINT NULL");
        ensureColumn("playwright_uname", "VARCHAR(128) NULL");
        ensureColumn("playwright_storage_state_json", "MEDIUMTEXT NULL");
        ensureColumn("playwright_updated_at", "DATETIME NULL");
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
        LoginSession session = loginSessions.remove(text(authCode));
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

    private record AccountProfile(Long mid, String uname) {
    }

    private record BrowserTypeOptions(boolean headless, String channel) {
        com.microsoft.playwright.BrowserType.LaunchOptions toLaunchOptions() {
            com.microsoft.playwright.BrowserType.LaunchOptions options = new com.microsoft.playwright.BrowserType.LaunchOptions()
                    .setHeadless(headless)
                    .setArgs(List.of("--no-sandbox", "--disable-dev-shm-usage"));
            if (channel != null && !channel.isBlank()) {
                options.setChannel(channel);
            }
            return options;
        }
    }

    private static class PlaywrightHolder {
        private static final Playwright PLAYWRIGHT = Playwright.create();

        static Playwright playwright() {
            return PLAYWRIGHT;
        }
    }
}
