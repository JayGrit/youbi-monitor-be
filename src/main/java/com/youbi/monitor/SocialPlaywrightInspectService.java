package com.youbi.monitor;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class SocialPlaywrightInspectService {
    private static final Logger log = LoggerFactory.getLogger(SocialPlaywrightInspectService.class);

    private final DouyinAccountService douyinAccountService;
    private final XiaohongshuAccountService xiaohongshuAccountService;
    private final BilibiliPlaywrightAccountService bilibiliAccountService;
    private final ShipinhaoAccountService shipinhaoAccountService;
    private final KuaishouAccountService kuaishouAccountService;
    private final JinritoutiaoAccountService jinritoutiaoAccountService;
    private final SocialBrowserFactory browserFactory;
    private final SocialRiskDetector riskDetector;
    private final DiagnosticArtifactService diagnosticArtifactService;
    private final Path douyinProfileRootDir;

    public SocialPlaywrightInspectService(
            DouyinAccountService douyinAccountService,
            XiaohongshuAccountService xiaohongshuAccountService,
            BilibiliPlaywrightAccountService bilibiliAccountService,
            ShipinhaoAccountService shipinhaoAccountService,
            KuaishouAccountService kuaishouAccountService,
            JinritoutiaoAccountService jinritoutiaoAccountService,
            SocialBrowserFactory browserFactory,
            SocialRiskDetector riskDetector,
            DiagnosticArtifactService diagnosticArtifactService,
            @Value("${youbi.douyin.profile-root-dir:/work/douyin-chrome-profiles}") String douyinProfileRootDir
    ) {
        this.douyinAccountService = douyinAccountService;
        this.xiaohongshuAccountService = xiaohongshuAccountService;
        this.bilibiliAccountService = bilibiliAccountService;
        this.shipinhaoAccountService = shipinhaoAccountService;
        this.kuaishouAccountService = kuaishouAccountService;
        this.jinritoutiaoAccountService = jinritoutiaoAccountService;
        this.browserFactory = browserFactory;
        this.riskDetector = riskDetector;
        this.diagnosticArtifactService = diagnosticArtifactService;
        this.douyinProfileRootDir = Path.of(text(douyinProfileRootDir).isBlank() ? "/work/douyin-chrome-profiles" : text(douyinProfileRootDir)).toAbsolutePath().normalize();
    }

    public Map<String, Object> fingerprint(String platformValue) {
        SocialBrowserPlatform platform = platform(platformValue);
        try (Browser browser = browserFactory.launchBrowser(platform)) {
            BrowserContext context = browserFactory.newContext(platform, browser);
            try {
                Page page = context.newPage();
                page.navigate("about:blank");
                @SuppressWarnings("unchecked")
                Map<String, Object> fingerprint = (Map<String, Object>) page.evaluate("""
                        () => ({
                          webdriver: navigator.webdriver,
                          languages: navigator.languages,
                          pluginsLength: navigator.plugins ? navigator.plugins.length : 0,
                          platform: navigator.platform,
                          userAgent: navigator.userAgent,
                          hardwareConcurrency: navigator.hardwareConcurrency,
                          deviceMemory: navigator.deviceMemory || null,
                          timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                          locale: Intl.DateTimeFormat().resolvedOptions().locale,
                          screen: { width: screen.width, height: screen.height, colorDepth: screen.colorDepth },
                          viewport: { width: window.innerWidth, height: window.innerHeight },
                          chromeRuntime: !!(window.chrome && window.chrome.runtime),
                          permissionsQuery: !!(navigator.permissions && navigator.permissions.query)
                        })
                        """);
                return Map.of(
                        "platform", platform.configKey(),
                        "passed", fingerprintPassed(fingerprint),
                        "fingerprint", fingerprint,
                        "warnings", fingerprintWarnings(fingerprint)
                );
            } finally {
                context.close();
            }
        }
    }

    public Map<String, Object> fingerprintAll() {
        Map<String, Object> results = new LinkedHashMap<>();
        for (SocialBrowserPlatform platform : SocialBrowserPlatform.values()) {
            try {
                results.put(platform.configKey(), fingerprint(platform.configKey()));
            } catch (Exception exception) {
                results.put(platform.configKey(), Map.of("message", exception.getMessage()));
            }
        }
        return results;
    }

    public Map<String, Object> inspectAllUploadPages(Map<String, String> accountKeys) {
        Map<String, Object> results = new LinkedHashMap<>();
        for (SocialBrowserPlatform platform : SocialBrowserPlatform.values()) {
            try {
                results.put(platform.configKey(), inspectUploadPage(platform.configKey(), accountKey(accountKeys, platform)));
            } catch (Exception exception) {
                results.put(platform.configKey(), Map.of("message", exception.getMessage()));
            }
        }
        return results;
    }

    public Map<String, Object> inspectUploadPage(String platformValue, String accountKey) throws IOException {
        SocialBrowserPlatform platform = platform(platformValue);
        String normalized = normalizeAccountKey(platform, accountKey);
        String taskId = platform.configKey() + "-inspect-" + UUID.randomUUID();
        if (platform == SocialBrowserPlatform.DOUYIN) {
            return inspectDouyinPersistentUploadPage(normalized, taskId);
        }
        String storageState = storageState(platform, normalized);
        BrowserHandle browserHandle = openBrowser(platform, normalized);
        try {
            BrowserContext context = context(platform, browserHandle.browser(), storageState);
            try {
                Page page = context.newPage();
                page.navigate(uploadUrl(platform));
                page.waitForTimeout(5000);
                DiagnosticArtifactRecord snapshot = diagnosticArtifactService.archive(new DiagnosticArtifactRequest(
                        page,
                        taskId,
                        taskId,
                        platform.configKey(),
                        "social-playwright-inspect",
                        normalized,
                        1,
                        "upload-page"
                ));
                SocialRiskState risk = riskDetector.detect(platform, page);
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("platform", platform.configKey());
                result.put("accountKey", normalized);
                result.put("url", page.url());
                result.put("risk", risk);
                result.put("ready", ready(platform, page, risk));
                result.put("screenshotUrl", snapshot.screenshotUrl());
                result.put("htmlUrl", snapshot.htmlUrl());
                result.put("archiveStatus", snapshot.status());
                result.put("archiveError", snapshot.errorMessage());
                result.put("fingerprint", fingerprintOnPage(page));
                return result;
            } finally {
                context.close();
            }
        } finally {
            browserHandle.close();
        }
    }

    private Map<String, Object> inspectDouyinPersistentUploadPage(String accountKey, String taskId) throws IOException {
        Path profileDir = douyinProfileRootDir.resolve(safeSegment(accountKey)).toAbsolutePath().normalize();
        Files.createDirectories(profileDir);
        try (BrowserContext context = douyinAccountService.launchPersistentContext(profileDir)) {
            Page page = context.newPage();
            page.navigate(uploadUrl(SocialBrowserPlatform.DOUYIN));
            page.waitForTimeout(5000);
            DiagnosticArtifactRecord snapshot = diagnosticArtifactService.archive(new DiagnosticArtifactRequest(
                    page,
                    taskId,
                    taskId,
                    SocialBrowserPlatform.DOUYIN.configKey(),
                    "social-playwright-inspect",
                    accountKey,
                    1,
                    "upload-page"
            ));
            SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.DOUYIN, page);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("platform", SocialBrowserPlatform.DOUYIN.configKey());
            result.put("accountKey", accountKey);
            result.put("url", page.url());
            result.put("risk", risk);
            result.put("ready", ready(SocialBrowserPlatform.DOUYIN, page, risk));
            result.put("screenshotUrl", snapshot.screenshotUrl());
            result.put("htmlUrl", snapshot.htmlUrl());
            result.put("archiveStatus", snapshot.status());
            result.put("archiveError", snapshot.errorMessage());
            result.put("fingerprint", fingerprintOnPage(page));
            return result;
        }
    }

    private boolean ready(SocialBrowserPlatform platform, Page page, SocialRiskState risk) {
        if (risk.blocking()) {
            return false;
        }
        String body = PlaywrightDiagnostics.safeBodyText(page);
        return switch (platform) {
            case DOUYIN -> containsAny(body, "上传视频", "发布视频", "点击上传", "拖拽到此处");
            case XIAOHONGSHU -> containsAny(body, "上传视频", "发布笔记", "请选择视频", "添加视频");
            case BILIBILI -> containsAny(body, "上传视频", "稿件", "发布", "点击上传");
            case SHIPINHAO -> containsAny(body, "发表动态", "视频描述", "发表", "保存草稿");
            case KUAISHOU -> containsAny(body, "发布视频", "上传视频", "选择视频", "作品描述");
            case JINRITOUTIAO -> containsAny(body, "发布视频", "点击上传", "基本信息", "视频简介");
        };
    }

    private Object fingerprintOnPage(Page page) {
        try {
            return page.evaluate("""
                    () => ({
                      webdriver: navigator.webdriver,
                      languages: navigator.languages,
                      pluginsLength: navigator.plugins ? navigator.plugins.length : 0,
                      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                      locale: Intl.DateTimeFormat().resolvedOptions().locale,
                      userAgent: navigator.userAgent
                    })
                    """);
        } catch (Exception exception) {
            return Map.of("error", exception.getMessage());
        }
    }

    private boolean fingerprintPassed(Map<String, Object> fingerprint) {
        Object webdriver = fingerprint.get("webdriver");
        Object pluginsLength = fingerprint.get("pluginsLength");
        return webdriver == null
                && pluginsLength instanceof Number number
                && number.intValue() > 0
                && String.valueOf(fingerprint.get("userAgent")).contains("Chrome")
                && !String.valueOf(fingerprint.get("userAgent")).contains("HeadlessChrome");
    }

    private Map<String, Object> fingerprintWarnings(Map<String, Object> fingerprint) {
        Map<String, Object> warnings = new LinkedHashMap<>();
        if (fingerprint.get("webdriver") != null) {
            warnings.put("webdriver", "navigator.webdriver should be undefined/null");
        }
        if (fingerprint.get("pluginsLength") instanceof Number number && number.intValue() <= 0) {
            warnings.put("plugins", "navigator.plugins is empty");
        }
        String userAgent = String.valueOf(fingerprint.get("userAgent"));
        if (userAgent.contains("HeadlessChrome")) {
            warnings.put("userAgent", "userAgent contains HeadlessChrome");
        }
        return warnings;
    }

    private BrowserHandle openBrowser(SocialBrowserPlatform platform, String accountKey) throws IOException {
        if (platform == SocialBrowserPlatform.BILIBILI) {
            BilibiliPlaywrightAccountService.BrowserHandle handle = bilibiliAccountService.openUploadBrowser();
            return new BrowserHandle(handle.browser(), handle::close);
        }
        Browser browser = browserFactory.launchBrowser(platform);
        return new BrowserHandle(browser, browser::close);
    }

    private BrowserContext context(SocialBrowserPlatform platform, Browser browser, String storageState) {
        return switch (platform) {
            case DOUYIN -> douyinAccountService.newContext(browser, douyinAccountService.storageContextOptions(storageState));
            case XIAOHONGSHU -> xiaohongshuAccountService.newContext(browser, storageState);
            case BILIBILI -> bilibiliAccountService.newContext(browser, storageState);
            case SHIPINHAO -> shipinhaoAccountService.newContext(browser, storageState);
            case KUAISHOU -> kuaishouAccountService.newContext(browser, storageState);
            case JINRITOUTIAO -> jinritoutiaoAccountService.newContext(browser, storageState);
        };
    }

    private String normalizeAccountKey(SocialBrowserPlatform platform, String accountKey) {
        return switch (platform) {
            case DOUYIN -> douyinAccountService.normalizeAccountKey(accountKey);
            case XIAOHONGSHU -> xiaohongshuAccountService.normalizeAccountKey(accountKey);
            case BILIBILI -> bilibiliAccountService.normalizeAccountKey(accountKey);
            case SHIPINHAO -> shipinhaoAccountService.normalizeAccountKey(accountKey);
            case KUAISHOU -> kuaishouAccountService.normalizeAccountKey(accountKey);
            case JINRITOUTIAO -> jinritoutiaoAccountService.normalizeAccountKey(accountKey);
        };
    }

    private String storageState(SocialBrowserPlatform platform, String accountKey) throws IOException {
        return switch (platform) {
            case DOUYIN -> douyinAccountService.storageState(accountKey);
            case XIAOHONGSHU -> xiaohongshuAccountService.storageState(accountKey);
            case BILIBILI -> bilibiliAccountService.storageState(accountKey);
            case SHIPINHAO -> shipinhaoAccountService.storageState(accountKey);
            case KUAISHOU -> kuaishouAccountService.storageState(accountKey);
            case JINRITOUTIAO -> jinritoutiaoAccountService.storageState(accountKey);
        };
    }

    private String uploadUrl(SocialBrowserPlatform platform) {
        return switch (platform) {
            case DOUYIN -> DouyinAccountService.PUBLISH_VIDEO_URL;
            case XIAOHONGSHU -> XiaohongshuAccountService.PUBLISH_VIDEO_URL;
            case BILIBILI -> BilibiliPlaywrightAccountService.PUBLISH_VIDEO_URL;
            case SHIPINHAO -> ShipinhaoAccountService.PUBLISH_VIDEO_URL;
            case KUAISHOU -> KuaishouAccountService.PUBLISH_VIDEO_URL;
            case JINRITOUTIAO -> JinritoutiaoAccountService.PUBLISH_VIDEO_URL;
        };
    }

    private String accountKey(Map<String, String> accountKeys, SocialBrowserPlatform platform) {
        if (accountKeys == null) {
            return "";
        }
        String direct = accountKeys.get(platform.configKey());
        if (direct != null) {
            return direct;
        }
        return accountKeys.get("accountKey");
    }

    private SocialBrowserPlatform platform(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase();
        return switch (normalized) {
            case "douyin", "dy" -> SocialBrowserPlatform.DOUYIN;
            case "xiaohongshu", "xhs" -> SocialBrowserPlatform.XIAOHONGSHU;
            case "bilibili", "bili" -> SocialBrowserPlatform.BILIBILI;
            case "shipinhao", "sph", "channels", "weixin-channels" -> SocialBrowserPlatform.SHIPINHAO;
            case "kuaishou", "ks" -> SocialBrowserPlatform.KUAISHOU;
            case "jinritoutiao", "toutiao", "tt" -> SocialBrowserPlatform.JINRITOUTIAO;
            default -> throw new IllegalArgumentException("Unsupported platform: " + value);
        };
    }

    private boolean containsAny(String text, String... keywords) {
        for (String keyword : keywords) {
            if (text.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    private String safeSegment(String value) {
        String sanitized = text(value).replaceAll("[^A-Za-z0-9._-]+", "_");
        return sanitized.isBlank() ? "default" : sanitized;
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record BrowserHandle(Browser browser, CloseAction closeAction) implements AutoCloseable {
        @Override
        public void close() {
            closeAction.close();
        }
    }

    @FunctionalInterface
    private interface CloseAction {
        void close();
    }

}
