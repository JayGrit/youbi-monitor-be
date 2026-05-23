package com.youbi.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.AriaRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DouyinAccountService {
    private static final Logger log = LoggerFactory.getLogger(DouyinAccountService.class);

    static final String DEFAULT_ACCOUNT_KEY = "default";
    static final String AUTO_ACCOUNT_KEY = "_auto";
    static final String LOGIN_URL = "https://creator.douyin.com/";
    static final String HOME_URL_PREFIX = "https://creator.douyin.com/creator-micro/home";
    static final String PUBLISH_VIDEO_URL = "https://creator.douyin.com/creator-micro/content/upload";

    private static final String TABLE = "yd_douyin_account";
    private static final String STEALTH_INIT_SCRIPT = """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh'] });
            window.chrome = window.chrome || { runtime: {} };
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final boolean headless;
    private final String browserChannel;
    private final String stealthInitScript;
    private final Map<String, LoginSession> loginSessions = new ConcurrentHashMap<>();

    public DouyinAccountService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            @Value("${youbi.douyin.headless:true}") boolean headless,
            @Value("${youbi.douyin.browser-channel:chrome}") String browserChannel,
            @Value("${youbi.douyin.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String stealthScriptPath
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.headless = headless;
        this.browserChannel = browserChannel == null || browserChannel.isBlank() ? "chrome" : browserChannel.trim();
        this.stealthInitScript = loadStealthInitScript(stealthScriptPath);
        ensureSchema();
    }

    public List<DouyinAccountStatus> accounts() {
        return jdbcTemplate.query(
                "SELECT account_key, user_id, nickname, storage_state_json, updated_at FROM " + TABLE + " ORDER BY account_key",
                (rs, rowNum) -> {
                    String json = rs.getString("storage_state_json");
                    return new DouyinAccountStatus(
                            "database",
                            rs.getString("account_key"),
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime(),
                            rs.getString("user_id"),
                            rs.getString("nickname"),
                            null,
                            "已保存",
                            Map.of()
                    );
                }
        );
    }

    public DouyinAccountStatus status(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        Optional<String> storageState = loadStorageState(normalized);
        if (storageState.isEmpty()) {
            return new DouyinAccountStatus("database", normalized, false, 0, null, null, null, false, "未登录", Map.of());
        }
        boolean valid = isStorageStateValid(storageState.get());
        LocalDateTime updatedAt = accountUpdatedAt(normalized).orElse(null);
        AccountProfile profile = loadProfile(normalized);
        return new DouyinAccountStatus(
                "database",
                normalized,
                true,
                storageState.get().getBytes(StandardCharsets.UTF_8).length,
                updatedAt,
                profile.userId(),
                profile.nickname(),
                valid,
                valid ? "已登录" : "cookie 已失效",
                Map.of()
        );
    }

    public String storageState(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        return loadStorageState(normalized)
                .orElseThrow(() -> new IOException("Douyin account is not logged in: " + normalized));
    }

    public DouyinQrCode createQrCode(String accountKey) throws IOException {
        String normalized = normalizeRequestedAccountKey(accountKey);
        String authCode = UUID.randomUUID().toString();
        closeSessionsForAccount(normalized);

        Playwright playwright = null;
        Browser browser = null;
        BrowserContext context = null;
        try {
            log.info("Creating Douyin QR login session accountKey={} authCode={}", normalized, authCode);
            playwright = Playwright.create();
            browser = playwright.chromium().launch(new BrowserTypeOptions(headless, browserChannel).toLaunchOptions());
            context = prepareContext(browser.newContext());
            Page page = context.newPage();
            page.navigate(LOGIN_URL);
            String originalUrl = page.url();
            String imageDataUrl = extractQrImage(page);
            loginSessions.put(authCode, new LoginSession(normalized, authCode, playwright, browser, context, page, originalUrl, Instant.now().plusSeconds(180)));
            log.info("Created Douyin QR login session accountKey={} authCode={} imageBytes={}", normalized, authCode, imageDataUrl.length());
            return new DouyinQrCode(normalized, authCode, imageDataUrl, Instant.now().getEpochSecond() + 180);
        } catch (Exception exception) {
            closeQuietly(context, browser, playwright);
            log.warn("Cannot create Douyin QR login session accountKey={} authCode={}: {}", normalized, authCode, exception.getMessage());
            throw new IOException("Cannot create Douyin qrcode: " + exception.getMessage(), exception);
        }
    }

    public DouyinQrPollResult pollQrCode(String accountKey, String authCode) throws IOException {
        LoginSession session = loginSessions.get(authCode);
        if (session == null) {
            return new DouyinQrPollResult(false, "missing", "二维码会话不存在或已过期", emptyStatus(normalizeRequestedAccountKey(accountKey)));
        }
        if (Instant.now().isAfter(session.expiresAt())) {
            closeSession(authCode);
            return new DouyinQrPollResult(false, "expired", "二维码已过期", emptyStatus(session.accountKey()));
        }

        try {
            String storageState = session.context().storageState();
            if (!isLoginCompleted(session.page(), storageState, session.originalUrl())) {
                if (isQrExpired(session.page())) {
                    closeSession(authCode);
                    return new DouyinQrPollResult(false, "expired", "二维码已过期，请重新扫码", emptyStatus(session.accountKey()));
                }
                log.info("Douyin QR login waiting accountKey={} authCode={} originalUrl={} url={} hasLoginCookie={}",
                        session.accountKey(), authCode, session.originalUrl(), session.page().url(), hasDouyinLoginCookie(storageState));
                return new DouyinQrPollResult(false, "waiting", "等待扫码确认", emptyStatus(session.accountKey()));
            }

            session.page().waitForTimeout(2000);
            storageState = session.context().storageState();
            String saveKey = AUTO_ACCOUNT_KEY.equals(session.accountKey()) ? automaticAccountKey(storageState) : session.accountKey();
            saveStorageState(saveKey, storageState);
            DouyinAccountStatus status = status(saveKey);
            if (!Boolean.TRUE.equals(status.valid())) {
                log.warn("Douyin QR login storage invalid accountKey={} authCode={} savedKey={} url={}",
                        session.accountKey(), authCode, saveKey, session.page().url());
                return new DouyinQrPollResult(false, "cookie_invalid", "扫码流程结束，但 cookie 校验失败", status);
            }
            closeSession(authCode);
            log.info("Douyin QR login saved accountKey={} authCode={} savedKey={} bytes={}",
                    session.accountKey(), authCode, saveKey, storageState.getBytes(StandardCharsets.UTF_8).length);
            return new DouyinQrPollResult(true, "success", "登录成功", status);
        } catch (Exception exception) {
            log.warn("Cannot poll Douyin QR login accountKey={} authCode={}: {}", session.accountKey(), authCode, exception.getMessage());
            throw new IOException("Cannot poll Douyin qrcode: " + exception.getMessage(), exception);
        }
    }

    public DouyinAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
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
            throw new IOException("Douyin account key already exists: " + newKey);
        }
        int updated = jdbcTemplate.update(
                "UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?",
                newKey,
                oldKey
        );
        if (updated != 1) {
            throw new IOException("Douyin account key not found: " + oldKey);
        }
        return status(newKey);
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Douyin accountKey: " + accountKey);
        }
        return normalized;
    }

    Browser launchBrowser() {
        return PlaywrightHolder.playwright().chromium().launch(new BrowserTypeOptions(headless, browserChannel).toLaunchOptions());
    }

    Browser connectBrowserOverCdp(String cdpUrl) {
        return PlaywrightHolder.playwright().chromium().connectOverCDP(cdpUrl);
    }

    BrowserContext firstContext(Browser browser) {
        List<BrowserContext> contexts = browser.contexts();
        if (contexts.isEmpty()) {
            return prepareContext(browser.newContext());
        }
        return contexts.get(0);
    }

    BrowserContext newContext(Browser browser) {
        return prepareContext(browser.newContext());
    }

    BrowserContext newContext(Browser browser, Browser.NewContextOptions options) {
        return prepareContext(browser.newContext(options));
    }

    Browser.NewContextOptions storageContextOptions(String storageState) {
        return new Browser.NewContextOptions()
                .setStorageState(storageState)
                .setPermissions(List.of("geolocation"));
    }

    void saveStorageState(String accountKey, String storageState) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        AccountProfile profile = profileFromStorageState(storageState);
        jdbcTemplate.update(
                """
                INSERT INTO yd_douyin_account (account_key, user_id, nickname, storage_state_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), nickname = VALUES(nickname), storage_state_json = VALUES(storage_state_json), updated_at = NOW()
                """,
                normalized,
                truncate(profile.userId(), 128),
                truncate(profile.nickname(), 128),
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

    private DouyinAccountStatus emptyStatus(String accountKey) {
        return new DouyinAccountStatus("database", accountKey, false, 0, null, null, null, false, "等待扫码", Map.of());
    }

    private String extractQrImage(Page page) {
        Locator scanLoginTab = page.getByText("扫码登录").first();
        scanLoginTab.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        try {
            if (scanLoginTab.isVisible()) {
                scanLoginTab.click(new Locator.ClickOptions().setTimeout(3000));
            }
        } catch (Exception ignored) {
        }

        String src = waitForVisibleQrImageSrc(page);
        if (src != null && !src.isBlank()) {
            return src;
        }
        Locator qrcode = page.locator("img").first();
        qrcode.waitFor(new Locator.WaitForOptions().setTimeout(3000));
        byte[] screenshot = qrcode.screenshot();
        return "data:image/png;base64," + java.util.Base64.getEncoder().encodeToString(screenshot);
    }

    private String waitForVisibleQrImageSrc(Page page) {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            String src = visibleQrImageSrc(page);
            if (!src.isBlank()) {
                return src;
            }
            page.waitForTimeout(500);
        }
        throw new com.microsoft.playwright.TimeoutError("Timed out waiting for visible Douyin QR image");
    }

    private String visibleQrImageSrc(Page page) {
        Object value = page.evaluate(
                """
                () => {
                  const visible = (el) => {
                    const rect = el.getBoundingClientRect();
                    const style = window.getComputedStyle(el);
                    return rect.width >= 120 && rect.height >= 120 &&
                      style.visibility !== 'hidden' && style.display !== 'none' &&
                      rect.bottom > 0 && rect.right > 0 &&
                      rect.top < window.innerHeight && rect.left < window.innerWidth;
                  };
                  const squareScore = (rect) => {
                    const ratio = rect.width / rect.height;
                    if (ratio < 0.82 || ratio > 1.22) return -1;
                    return Math.min(rect.width, rect.height);
                  };
                  const imgs = Array.from(document.querySelectorAll('img'));
                  let best = null;
                  let bestScore = -1;
                  imgs.forEach((img) => {
                    if (!visible(img)) return;
                    const rect = img.getBoundingClientRect();
                    const baseScore = squareScore(rect);
                    if (baseScore < 0) return;
                    const src = img.getAttribute('src') || '';
                    let score = baseScore;
                    if (src.startsWith('data:image/png') || src.startsWith('data:image/jpeg') || src.startsWith('data:image/webp')) score += 2000;
                    if (src.startsWith('data:image/svg')) score -= 500;
                    if ((img.getAttribute('aria-label') || '').includes('二维码')) score += 500;
                    if ((img.alt || '').includes('二维码')) score += 500;
                    if (score > bestScore) {
                      best = src;
                      bestScore = score;
                    }
                  });
                  Array.from(document.querySelectorAll('canvas')).forEach((canvas) => {
                    if (!visible(canvas)) return;
                    const rect = canvas.getBoundingClientRect();
                    const baseScore = squareScore(rect);
                    if (baseScore < 0) return;
                    try {
                      const src = canvas.toDataURL('image/png');
                      const score = baseScore + 2200;
                      if (score > bestScore) {
                        best = src;
                        bestScore = score;
                      }
                    } catch (_) {}
                  });
                  return best || '';
                }
                """
        );
        return value == null ? "" : value.toString();
    }

    private boolean isLoginCompleted(Page page, String storageState, String originalUrl) {
        if (page.url().startsWith(HOME_URL_PREFIX)) {
            return true;
        }
        if (!page.url().equals(originalUrl) && !hasVisibleLoginMarker(page)) {
            return true;
        }
        return hasDouyinLoginCookie(storageState) && !hasVisibleLoginMarker(page);
    }

    private boolean hasVisibleLoginMarker(Page page) {
        for (Locator marker : List.of(
                page.getByText("扫码登录", new Page.GetByTextOptions().setExact(true)).first(),
                page.getByText("手机号登录", new Page.GetByTextOptions().setExact(true)).first(),
                page.getByText("二维码失效", new Page.GetByTextOptions().setExact(true)).first(),
                page.getByRole(AriaRole.IMG, new Page.GetByRoleOptions().setName("二维码")).first()
        )) {
            if (marker.count() == 0) {
                continue;
            }
            try {
                if (marker.isVisible()) {
                    return false;
                }
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private boolean hasDouyinLoginCookie(String storageState) {
        try {
            JsonNode root = objectMapper.readTree(storageState);
            for (JsonNode cookie : root.path("cookies")) {
                String name = cookie.path("name").asText("");
                String value = cookie.path("value").asText("");
                if (value.isBlank()) {
                    continue;
                }
                if (List.of("sessionid", "sessionid_ss", "sid_tt", "uid_tt", "sso_uid_tt", "passport_assist_user").contains(name)) {
                    return true;
                }
            }
        } catch (Exception ignored) {
        }
        return false;
    }

    private boolean isQrExpired(Page page) {
        try {
            Locator expired = page.getByText("二维码失效", new Page.GetByTextOptions().setExact(true)).first();
            return expired.count() > 0 && expired.isVisible();
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean isStorageStateValid(String storageState) {
        try (Browser browser = launchBrowser()) {
            BrowserContext context = newContext(browser, storageContextOptions(storageState));
            try {
                Page page = context.newPage();
                page.navigate(PUBLISH_VIDEO_URL);
                try {
                    page.waitForURL(PUBLISH_VIDEO_URL, new Page.WaitForURLOptions().setTimeout(8000));
                } catch (Exception ignored) {
                }
                long deadline = System.currentTimeMillis() + 15000;
                while (System.currentTimeMillis() < deadline) {
                    if (hasUploadFileInput(page)) {
                        return true;
                    }
                    if (hasVisibleLoginGate(page)) {
                        return false;
                    }
                    page.waitForTimeout(500);
                }
                return hasUploadFileInput(page);
            } finally {
                context.close();
            }
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean hasUploadFileInput(Page page) {
        try {
            return page.locator("input[type='file']").count() > 0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean hasVisibleLoginGate(Page page) {
        for (Locator marker : List.of(
                page.getByText("扫码登录").first(),
                page.getByText("手机号登录").first(),
                page.getByText("验证码登录").first(),
                page.getByText("创作者登录").first(),
                page.getByText("登录/注册").first(),
                page.getByRole(AriaRole.IMG, new Page.GetByRoleOptions().setName("二维码")).first(),
                page.locator("input[placeholder*='手机号']").first(),
                page.locator("input[placeholder*='验证码']").first()
        )) {
            try {
                if (marker.count() > 0 && marker.isVisible()) {
                    return true;
                }
            } catch (Exception ignored) {
            }
        }
        return false;
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
        for (JsonNode cookie : root.path("cookies")) {
            String name = cookie.path("name").asText("");
            String value = cookie.path("value").asText("");
            if ("passport_assist_user".equals(name) || "sso_uid_tt".equals(name) || "uid_tt".equals(name)) {
                userId = firstText(userId, value);
            }
        }
        for (JsonNode origin : root.path("origins")) {
            for (JsonNode item : origin.path("localStorage")) {
                String name = item.path("name").asText("");
                String value = item.path("value").asText("");
                if ((name.toLowerCase().contains("user") || value.contains("nickname")) && !value.isBlank()) {
                    try {
                        JsonNode parsed = objectMapper.readTree(value);
                        userId = firstText(userId, parsed.path("userId").asText(""), parsed.path("user_id").asText(""), parsed.path("uid").asText(""), parsed.path("id").asText(""));
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
                CREATE TABLE IF NOT EXISTS yd_douyin_account (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    user_id VARCHAR(128) NULL,
                    nickname VARCHAR(128) NULL,
                    storage_state_json MEDIUMTEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
        );
    }

    private void closeSession(String authCode) {
        LoginSession session = loginSessions.remove(authCode);
        if (session == null) {
            return;
        }
        closeQuietly(session.context(), session.browser(), session.playwright());
    }

    private void closeSessionsForAccount(String accountKey) {
        loginSessions.values().stream()
                .filter(session -> session.accountKey().equals(accountKey))
                .map(LoginSession::authCode)
                .toList()
                .forEach(this::closeSession);
    }

    private void closeQuietly(BrowserContext context, Browser browser, Playwright playwright) {
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
        try {
            if (playwright != null) {
                playwright.close();
            }
        } catch (Exception ignored) {
        }
    }

    private BrowserContext prepareContext(BrowserContext context) {
        context.addInitScript(stealthInitScript);
        return context;
    }

    private String loadStealthInitScript(String stealthScriptPath) {
        String path = text(stealthScriptPath);
        if (!path.isBlank()) {
            try {
                Path script = Path.of(path);
                if (Files.isRegularFile(script)) {
                    String content = Files.readString(script, StandardCharsets.UTF_8);
                    log.info("Loaded Douyin stealth init script path={} bytes={}", script, content.getBytes(StandardCharsets.UTF_8).length);
                    return content;
                }
                log.warn("Douyin stealth init script not found path={}, using fallback", script);
            } catch (Exception exception) {
                log.warn("Cannot load Douyin stealth init script path={}, using fallback: {}", path, exception.getMessage());
            }
        }
        return STEALTH_INIT_SCRIPT;
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

    private String truncate(String value, int max) {
        String text = text(value);
        if (text.isBlank()) {
            return null;
        }
        if (text.codePointCount(0, text.length()) <= max) {
            return text;
        }
        return text.codePoints()
                .limit(max)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record LoginSession(String accountKey, String authCode, Playwright playwright, Browser browser,
                                BrowserContext context, Page page, String originalUrl, Instant expiresAt) {
    }

    private record AccountProfile(String userId, String nickname) {
    }

    private record BrowserTypeOptions(boolean headless, String channel) {
        com.microsoft.playwright.BrowserType.LaunchOptions toLaunchOptions() {
            return new com.microsoft.playwright.BrowserType.LaunchOptions()
                    .setHeadless(headless)
                    .setChannel(channel)
                    .setArgs(List.of(
                            "--no-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-blink-features=AutomationControlled",
                            "--lang=zh-CN",
                            "--disable-infobars",
                            "--start-maximized"
                    ));
        }
    }

    private static class PlaywrightHolder {
        private static final Playwright PLAYWRIGHT = Playwright.create();

        static Playwright playwright() {
            return PLAYWRIGHT;
        }
    }
}
