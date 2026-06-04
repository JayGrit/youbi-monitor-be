package com.youbi.monitor.service;

import com.youbi.monitor.dto.AccountSendAvailability;
import com.youbi.monitor.dto.BilibiliPlaywrightAccountStatus;
import com.youbi.monitor.dto.BilibiliPlaywrightLoginOpenResult;
import com.youbi.monitor.dto.BilibiliPlaywrightLoginPollResult;
import com.youbi.monitor.dto.UploaderAccountState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import org.springframework.beans.factory.annotation.Value;
import com.youbi.monitor.repository.DatabaseClient;
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
    public static final String DEFAULT_ACCOUNT_KEY = "knowledge";
    static final String PUBLISH_VIDEO_URL = "https://member.bilibili.com/platform/upload/video/frame?page_from=creative_home_top_upload";

    private static final String TABLE = "uploader_account_bilibili";

    private final DatabaseClient jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final UploaderAccountService uploaderAccountService;
    private final HttpClient httpClient;
    private final SocialBrowserFactory browserFactory;
    private final String cdpUrl;
    private final Map<String, LoginSession> loginSessions = new ConcurrentHashMap<>();

    public BilibiliPlaywrightAccountService(
            DatabaseClient jdbcTemplate,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            UploaderAccountService uploaderAccountService,
            SocialBrowserFactory browserFactory,
            @Value("${youbi.bilibili.playwright.cdp-url:}") String cdpUrl
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.uploaderAccountService = uploaderAccountService;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).followRedirects(HttpClient.Redirect.NORMAL).build();
        this.browserFactory = browserFactory;
        this.cdpUrl = text(cdpUrl);
        ensureSchema();
    }

    public BilibiliPlaywrightLoginOpenResult openManualLogin(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        String authCode = UUID.randomUUID().toString();
        closeSession(authCode);
        Browser browser = null;
        BrowserContext context = null;
        try {
            browser = launchBrowser();
            Optional<String> storageState = loadStorageState(normalized);
            context = storageState.isPresent()
                    ? browserFactory.newContext(SocialBrowserPlatform.BILIBILI, browser, storageState.get())
                    : browserFactory.newContext(SocialBrowserPlatform.BILIBILI, browser);
            Page page = context.newPage();
            page.navigate(PUBLISH_VIDEO_URL);
            loginSessions.put(authCode, new LoginSession(normalized, authCode, browser, context, page, Instant.now().plusSeconds(900)));
            return new BilibiliPlaywrightLoginOpenResult(normalized, authCode, page.url(), Instant.now().getEpochSecond() + 900);
        } catch (Exception exception) {
            closeQuietly(context, browser);
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
                FROM uploader_account_bilibili
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
                            sendAvailability.todayUploadCount(),
                            sendAvailability.cooldownWaitingCount(),
                            sendAvailability.uploadRunningCount(),
                            accountEnabled(accountKey),
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
        return browserFactory.launchBrowser(SocialBrowserPlatform.BILIBILI);
    }

    BrowserHandle openUploadBrowser() throws IOException {
        if (!cdpUrl.isBlank()) {
            return new BrowserHandle(browserFactory.connectOverCdp(browserWsUrl(cdpUrl)), false);
        }
        return new BrowserHandle(launchBrowser(), true);
    }

    Browser.NewContextOptions storageContextOptions(String storageState) {
        return browserFactory.storageContextOptions(SocialBrowserPlatform.BILIBILI, storageState);
    }

    BrowserContext newContext(Browser browser, String storageState) {
        return browserFactory.newContext(SocialBrowserPlatform.BILIBILI, browser, storageState);
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        saveStorageState(normalizeAccountKey(accountKey), storageState, profileFromCookieState(storageState).orElse(new AccountProfile(null, null)));
    }

    private void saveStorageState(String accountKey, String storageState, AccountProfile profile) {
        int updated = jdbcTemplate.update(
                """
                UPDATE uploader_account_bilibili
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

    private String browserWsUrl(String endpoint) throws IOException {
        String value = text(endpoint);
        if (value.startsWith("ws://") || value.startsWith("wss://")) {
            return value;
        }
        URI base;
        try {
            base = URI.create(value);
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid Bilibili Playwright CDP URL: " + endpoint, exception);
        }
        HttpRequest request = HttpRequest.newBuilder(base.resolve("/json/version"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() / 100 != 2) {
                throw new IOException("Bilibili Playwright CDP version endpoint returned HTTP " + response.statusCode());
            }
            String browserWsUrl = objectMapper.readTree(response.body()).path("webSocketDebuggerUrl").asText("");
            if (browserWsUrl.isBlank()) {
                throw new IOException("Bilibili Playwright CDP version endpoint missing webSocketDebuggerUrl");
            }
            return browserWsUrl;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted resolving Bilibili Playwright CDP endpoint", exception);
        }
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
        return new BilibiliPlaywrightAccountStatus("database", accountKey, false, 0, null, null, null, null, null, 0, 0, 0, true, false, message, Map.of());
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("bilibili", accountKey, "uploader_account_bilibili");
    }

    private boolean accountEnabled(String accountKey) {
        return uploaderAccountService.state("bilibili", accountKey)
                .map(UploaderAccountState::enabled)
                .orElse(true);
    }

    private void ensureSchema() {
        AccountTableSchemaSupport.ensureSurrogatePrimaryKey(jdbcTemplate, TABLE);
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

    private record AccountProfile(Long mid, String uname) {
    }

    static class BrowserHandle implements AutoCloseable {
        private final Browser browser;
        private final boolean closeBrowser;

        BrowserHandle(Browser browser, boolean closeBrowser) {
            this.browser = browser;
            this.closeBrowser = closeBrowser;
        }

        Browser browser() {
            return browser;
        }

        boolean browserSideFiles() {
            return !closeBrowser;
        }

        @Override
        public void close() {
            if (closeBrowser) {
                browser.close();
            }
        }
    }

}
