package com.youbi.monitor.service;

import com.youbi.monitor.dto.AccountSendAvailability;
import com.youbi.monitor.dto.ShipinhaoAccountStatus;
import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.model.SocialAccountProfile;
import com.youbi.monitor.repository.IShipinhaoAccountRepositoryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    private final IShipinhaoAccountRepositoryService repositoryService;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final SocialBrowserFactory browserFactory;
    private final UploaderAccountService uploaderAccountService;

    public ShipinhaoAccountService(
            IShipinhaoAccountRepositoryService repositoryService,
            ObjectMapper objectMapper,
            AccountSendAvailabilityService sendAvailabilityService,
            SocialBrowserFactory browserFactory,
            UploaderAccountService uploaderAccountService
    ) {
        this.repositoryService = repositoryService;
        this.objectMapper = objectMapper;
        this.sendAvailabilityService = sendAvailabilityService;
        this.browserFactory = browserFactory;
        this.uploaderAccountService = uploaderAccountService;
        ensureSchema();
    }

    public List<ShipinhaoAccountStatus> accounts() {
        return repositoryService.listAccounts();
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
        SocialAccountProfile profile = loadProfile(normalized);
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

    @Transactional(rollbackFor = Exception.class)
    public ShipinhaoAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
        String oldKey = normalizeAccountKey(oldAccountKey);
        String newKey = normalizeAccountKey(newAccountKey);
        if (oldKey.equals(newKey)) {
            return status(oldKey);
        }
        if (repositoryService.existsAccountKey(newKey)) {
            throw new IOException("Shipinhao account key already exists: " + newKey);
        }
        if (!repositoryService.renameAccountKey(oldKey, newKey)) {
            throw new IOException("Shipinhao account key not found: " + oldKey);
        }
        if (!uploaderAccountService.renameAccountKey("shipinhao", oldKey, newKey)) {
            throw new IOException("Shipinhao uploader account key not found: " + oldKey);
        }
        return status(newKey);
    }

    public ShipinhaoAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Shipinhao account key not found: " + normalized);
        }
        if (!uploaderAccountService.updateEnabled("shipinhao", normalized, enabled)) {
            throw new IOException("Shipinhao uploader account key not found: " + normalized);
        }
        return status(normalized);
    }

    public ShipinhaoAccountStatus setCooldown(String accountKey, Integer minSeconds, Integer maxSeconds) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int[] cooldown = normalizeCooldown(minSeconds, maxSeconds);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Shipinhao account key not found: " + normalized);
        }
        if (!uploaderAccountService.updateCooldown("shipinhao", normalized, cooldown[0], cooldown[1])) {
            throw new IOException("Shipinhao uploader account key not found: " + normalized);
        }
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
        SocialAccountProfile profile = profileFromStorageState(storageState);
        repositoryService.saveStorageState(
                normalized,
                firstText(profile.userId(), loadProfile(normalized).userId()),
                firstText(profile.nickname(), loadProfile(normalized).nickname()),
                storageState
        );
    }

    void markUnavailable(String accountKey) {
        String normalized = normalizeAccountKey(accountKey);
        repositoryService.markUnavailable(normalized);
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
        return repositoryService.findStorageState(accountKey);
    }

    private Optional<LocalDateTime> accountUpdatedAt(String accountKey) {
        return repositoryService.findUpdatedAt(accountKey);
    }

    private SocialAccountProfile loadProfile(String accountKey) {
        return repositoryService.findProfile(accountKey);
    }

    private SocialAccountProfile profileFromStorageState(String storageState) throws IOException {
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
        return new SocialAccountProfile(blankToNull(userId), blankToNull(nickname));
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

    private boolean accountKeyExists(String accountKey) {
        return repositoryService.existsAccountKey(accountKey);
    }

    private void ensureSchema() {
        repositoryService.ensureSchema();
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
}
