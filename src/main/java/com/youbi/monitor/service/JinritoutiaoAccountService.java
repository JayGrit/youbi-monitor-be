package com.youbi.monitor.service;

import com.youbi.monitor.dto.AccountSendAvailability;
import com.youbi.monitor.dto.JinritoutiaoAccountStatus;
import com.youbi.monitor.dto.UploaderAccountState;
import com.youbi.monitor.model.SocialAccountProfile;
import com.youbi.monitor.repository.IJinritoutiaoAccountRepositoryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
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

    private final IJinritoutiaoAccountRepositoryService repositoryService;
    private final ObjectMapper objectMapper;
    private final AccountSendAvailabilityService sendAvailabilityService;
    private final SocialBrowserFactory browserFactory;
    private final UploaderAccountService uploaderAccountService;

    public JinritoutiaoAccountService(
            IJinritoutiaoAccountRepositoryService repositoryService,
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

    public List<JinritoutiaoAccountStatus> accounts() {
        return repositoryService.listAccounts();
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
        SocialAccountProfile profile = loadProfile(normalized);
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
        if (repositoryService.existsAccountKey(newKey)) {
            throw new IOException("Jinritoutiao account key already exists: " + newKey);
        }
        if (!repositoryService.renameAccountKey(oldKey, newKey)) {
            throw new IOException("Jinritoutiao account key not found: " + oldKey);
        }
        uploaderAccountService.renameAccount("jinritoutiao", oldKey, newKey);
        return status(newKey);
    }

    public JinritoutiaoAccountStatus setEnabled(String accountKey, boolean enabled) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Jinritoutiao account key not found: " + normalized);
        }
        uploaderAccountService.updateEnabled("jinritoutiao", normalized, enabled);
        return status(normalized);
    }

    public JinritoutiaoAccountStatus setCooldown(String accountKey, Integer minSeconds, Integer maxSeconds) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        int[] cooldown = normalizeCooldown(minSeconds, maxSeconds);
        if (!accountKeyExists(normalized)) {
            throw new IOException("Jinritoutiao account key not found: " + normalized);
        }
        uploaderAccountService.updateCooldown("jinritoutiao", normalized, cooldown[0], cooldown[1]);
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
        SocialAccountProfile profile = profileFromStorageState(storageState);
        repositoryService.saveStorageState(
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
        return new SocialAccountProfile(blankToNull(userId), blankToNull(nickname));
    }

    private int[] cooldownConfig(String accountKey) {
        UploaderAccountState state = uploaderAccountService.state("jinritoutiao", accountKey)
                .orElseGet(() -> UploaderAccountState.defaults("jinritoutiao", accountKey));
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
        return uploaderAccountService.state("jinritoutiao", accountKey)
                .map(UploaderAccountState::enabled)
                .orElse(true);
    }

    private AccountSendAvailability sendAvailability(String accountKey) {
        return sendAvailabilityService.availability("jinritoutiao", accountKey, TABLE);
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
                "jinritoutiao",
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
