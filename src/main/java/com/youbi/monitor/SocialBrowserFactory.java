package com.youbi.monitor;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Playwright;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class SocialBrowserFactory {
    private static final Logger log = LoggerFactory.getLogger(SocialBrowserFactory.class);

    private static final String FALLBACK_STEALTH_INIT_SCRIPT = """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = window.chrome || { runtime: {} };
            """;

    private final Playwright playwright;
    private final BrowserProfile douyinProfile;
    private final BrowserProfile xiaohongshuProfile;
    private final BrowserProfile bilibiliProfile;
    private final BrowserProfile shipinhaoProfile;
    private final BrowserProfile kuaishouProfile;
    private final BrowserProfile jinritoutiaoProfile;
    private final String defaultLocale;
    private final String defaultTimezone;
    private final int defaultViewportWidth;
    private final int defaultViewportHeight;
    private final String acceptLanguage;
    private final String defaultUserAgent;

    public SocialBrowserFactory(
            @Value("${youbi.playwright.default-locale:zh-CN}") String defaultLocale,
            @Value("${youbi.playwright.default-timezone:Asia/Shanghai}") String defaultTimezone,
            @Value("${youbi.playwright.default-viewport-width:1400}") int defaultViewportWidth,
            @Value("${youbi.playwright.default-viewport-height:900}") int defaultViewportHeight,
            @Value("${youbi.playwright.accept-language:zh-CN,zh;q=0.9,en;q=0.8}") String acceptLanguage,
            @Value("${youbi.playwright.user-agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36}") String defaultUserAgent,
            @Value("${youbi.douyin.headless:true}") boolean douyinHeadless,
            @Value("${youbi.douyin.browser-channel:chrome}") String douyinBrowserChannel,
            @Value("${youbi.douyin.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String douyinStealthScriptPath,
            @Value("${youbi.xiaohongshu.headless:true}") boolean xiaohongshuHeadless,
            @Value("${youbi.xiaohongshu.browser-channel:chrome}") String xiaohongshuBrowserChannel,
            @Value("${youbi.xiaohongshu.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String xiaohongshuStealthScriptPath,
            @Value("${youbi.bilibili.playwright.headless:false}") boolean bilibiliHeadless,
            @Value("${youbi.bilibili.playwright.browser-channel:chrome}") String bilibiliBrowserChannel,
            @Value("${youbi.bilibili.playwright.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String bilibiliStealthScriptPath,
            @Value("${youbi.shipinhao.headless:true}") boolean shipinhaoHeadless,
            @Value("${youbi.shipinhao.browser-channel:chrome}") String shipinhaoBrowserChannel,
            @Value("${youbi.shipinhao.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String shipinhaoStealthScriptPath,
            @Value("${youbi.kuaishou.headless:true}") boolean kuaishouHeadless,
            @Value("${youbi.kuaishou.browser-channel:chrome}") String kuaishouBrowserChannel,
            @Value("${youbi.kuaishou.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String kuaishouStealthScriptPath,
            @Value("${youbi.jinritoutiao.headless:true}") boolean jinritoutiaoHeadless,
            @Value("${youbi.jinritoutiao.browser-channel:chrome}") String jinritoutiaoBrowserChannel,
            @Value("${youbi.jinritoutiao.stealth-script-path:/Users/hoshuuch/Money/social-auto-upload/utils/stealth.min.js}") String jinritoutiaoStealthScriptPath
    ) {
        this.playwright = Playwright.create();
        this.defaultLocale = text(defaultLocale).isBlank() ? "zh-CN" : text(defaultLocale);
        this.defaultTimezone = text(defaultTimezone).isBlank() ? "Asia/Shanghai" : text(defaultTimezone);
        this.defaultViewportWidth = defaultViewportWidth <= 0 ? 1400 : defaultViewportWidth;
        this.defaultViewportHeight = defaultViewportHeight <= 0 ? 900 : defaultViewportHeight;
        this.acceptLanguage = text(acceptLanguage).isBlank() ? "zh-CN,zh;q=0.9,en;q=0.8" : text(acceptLanguage);
        this.defaultUserAgent = text(defaultUserAgent);
        this.douyinProfile = new BrowserProfile(douyinHeadless, channel(douyinBrowserChannel), loadStealthScript(SocialBrowserPlatform.DOUYIN, douyinStealthScriptPath));
        this.xiaohongshuProfile = new BrowserProfile(xiaohongshuHeadless, channel(xiaohongshuBrowserChannel), loadStealthScript(SocialBrowserPlatform.XIAOHONGSHU, xiaohongshuStealthScriptPath));
        this.bilibiliProfile = new BrowserProfile(bilibiliHeadless, channel(bilibiliBrowserChannel), loadStealthScript(SocialBrowserPlatform.BILIBILI, bilibiliStealthScriptPath));
        this.shipinhaoProfile = new BrowserProfile(shipinhaoHeadless, channel(shipinhaoBrowserChannel), loadStealthScript(SocialBrowserPlatform.SHIPINHAO, shipinhaoStealthScriptPath));
        this.kuaishouProfile = new BrowserProfile(kuaishouHeadless, channel(kuaishouBrowserChannel), loadStealthScript(SocialBrowserPlatform.KUAISHOU, kuaishouStealthScriptPath));
        this.jinritoutiaoProfile = new BrowserProfile(jinritoutiaoHeadless, channel(jinritoutiaoBrowserChannel), loadStealthScript(SocialBrowserPlatform.JINRITOUTIAO, jinritoutiaoStealthScriptPath));
    }

    Browser launchBrowser(SocialBrowserPlatform platform) {
        return playwright.chromium().launch(launchOptions(platform));
    }

    Browser connectOverCdp(String browserWsUrl) {
        return playwright.chromium().connectOverCDP(browserWsUrl);
    }

    BrowserContext launchPersistentContext(SocialBrowserPlatform platform, Path userDataDir) {
        BrowserProfile profile = profile(platform);
        BrowserType.LaunchPersistentContextOptions options = new BrowserType.LaunchPersistentContextOptions()
                .setHeadless(profile.headless())
                .setArgs(launchArgs(platform))
                .setLocale(defaultLocale)
                .setTimezoneId(defaultTimezone)
                .setViewportSize(defaultViewportWidth, defaultViewportHeight)
                .setExtraHTTPHeaders(Map.of("Accept-Language", acceptLanguage));
        if (!profile.channel().isBlank()) {
            options.setChannel(profile.channel());
        }
        if (!defaultUserAgent.isBlank()) {
            options.setUserAgent(defaultUserAgent);
        }
        if (platform == SocialBrowserPlatform.DOUYIN) {
            options.setPermissions(List.of("geolocation"));
        }
        return prepareContext(platform, playwright.chromium().launchPersistentContext(userDataDir, options));
    }

    BrowserContext newContext(SocialBrowserPlatform platform, Browser browser) {
        return prepareContext(platform, browser.newContext(defaultContextOptions(platform, null)));
    }

    BrowserContext newContext(SocialBrowserPlatform platform, Browser browser, String storageState) {
        return prepareContext(platform, browser.newContext(defaultContextOptions(platform, storageState)));
    }

    BrowserContext firstOrNewContext(SocialBrowserPlatform platform, Browser browser) {
        List<BrowserContext> contexts = browser.contexts();
        if (contexts.isEmpty()) {
            return newContext(platform, browser);
        }
        return prepareContext(platform, contexts.get(0));
    }

    Browser.NewContextOptions storageContextOptions(SocialBrowserPlatform platform, String storageState) {
        return defaultContextOptions(platform, storageState);
    }

    BrowserContext prepareContext(SocialBrowserPlatform platform, BrowserContext context) {
        String script = profile(platform).stealthInitScript();
        if (!script.isBlank()) {
            context.addInitScript(script);
        }
        return context;
    }

    @PreDestroy
    void close() {
        playwright.close();
    }

    private BrowserType.LaunchOptions launchOptions(SocialBrowserPlatform platform) {
        BrowserProfile profile = profile(platform);
        BrowserType.LaunchOptions options = new BrowserType.LaunchOptions()
                .setHeadless(profile.headless())
                .setArgs(launchArgs(platform));
        if (!profile.channel().isBlank()) {
            options.setChannel(profile.channel());
        }
        return options;
    }

    private Browser.NewContextOptions defaultContextOptions(SocialBrowserPlatform platform, String storageState) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Accept-Language", acceptLanguage);
        Browser.NewContextOptions options = new Browser.NewContextOptions()
                .setLocale(defaultLocale)
                .setTimezoneId(defaultTimezone)
                .setViewportSize(defaultViewportWidth, defaultViewportHeight)
                .setExtraHTTPHeaders(headers);
        if (!defaultUserAgent.isBlank()) {
            options.setUserAgent(defaultUserAgent);
        }
        if (platform == SocialBrowserPlatform.DOUYIN) {
            options.setPermissions(List.of("geolocation"));
        }
        if (storageState != null && !storageState.isBlank()) {
            options.setStorageState(storageState);
        }
        return options;
    }

    private List<String> launchArgs(SocialBrowserPlatform platform) {
        List<String> args = new ArrayList<>(List.of(
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--lang=" + defaultLocale,
                "--window-size=" + defaultViewportWidth + "," + defaultViewportHeight
        ));
        if (platform == SocialBrowserPlatform.DOUYIN) {
            args.add("--start-maximized");
        }
        return args;
    }

    private BrowserProfile profile(SocialBrowserPlatform platform) {
        return switch (platform) {
            case DOUYIN -> douyinProfile;
            case XIAOHONGSHU -> xiaohongshuProfile;
            case BILIBILI -> bilibiliProfile;
            case SHIPINHAO -> shipinhaoProfile;
            case KUAISHOU -> kuaishouProfile;
            case JINRITOUTIAO -> jinritoutiaoProfile;
        };
    }

    private String loadStealthScript(SocialBrowserPlatform platform, String stealthScriptPath) {
        String path = text(stealthScriptPath);
        if (!path.isBlank()) {
            try {
                Path script = Path.of(path);
                if (Files.isRegularFile(script)) {
                    String content = Files.readString(script, StandardCharsets.UTF_8);
                    log.info("Loaded {} stealth init script path={} bytes={}", platform.configKey(), script, content.getBytes(StandardCharsets.UTF_8).length);
                    return content;
                }
                log.warn("{} stealth init script not found path={}, using fallback", platform.configKey(), script);
            } catch (Exception exception) {
                log.warn("Cannot load {} stealth init script path={}, using fallback: {}", platform.configKey(), path, exception.getMessage());
            }
        }
        return FALLBACK_STEALTH_INIT_SCRIPT;
    }

    private String channel(String value) {
        String channel = text(value);
        return channel.isBlank() ? "chrome" : channel;
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record BrowserProfile(boolean headless, String channel, String stealthInitScript) {
    }
}
