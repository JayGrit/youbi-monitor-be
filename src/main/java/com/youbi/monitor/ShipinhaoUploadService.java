package com.youbi.monitor;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.TimeoutError;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service
public class ShipinhaoUploadService {
    private static final Logger log = LoggerFactory.getLogger(ShipinhaoUploadService.class);
    private static final String DIAGNOSTIC_PLATFORM = "shipinhao";
    private static final String DIAGNOSTIC_SOURCE = "shipinhao-upload";

    private final ShipinhaoAccountService accountService;
    private final UploadMaterialResolver materialResolver;
    private final DiagnosticArtifactService diagnosticArtifactService;
    private final ThreadLocal<DiagnosticRunContext> diagnosticContext = new ThreadLocal<>();

    public ShipinhaoUploadService(
            ShipinhaoAccountService accountService,
            DiagnosticArtifactService diagnosticArtifactService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir
    ) {
        this.accountService = accountService;
        this.diagnosticArtifactService = diagnosticArtifactService;
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        String bucket = TextSupport.text(minioBucket).isBlank() ? "ydbi" : TextSupport.text(minioBucket);
        Path workDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).followRedirects(HttpClient.Redirect.NORMAL).build();
        this.materialResolver = new UploadMaterialResolver(minioClient, bucket, workDir, httpClient, log, "Shipinhao upload", true);
    }

    public ShipinhaoUploadResult upload(ShipinhaoUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = TextSupport.firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        UploadMaterialResolver.ResolvedFile resolvedVideo = resolveVideo(request);
        DiagnosticRunContext diagnostics = new DiagnosticRunContext(taskId, taskId, DIAGNOSTIC_PLATFORM, DIAGNOSTIC_SOURCE, accountKey);
        diagnosticContext.set(diagnostics);
        try {
            Path videoPath = resolvedVideo.path();
            if (!Files.isRegularFile(videoPath) || Files.size(videoPath) == 0) {
                throw new IOException("Video file does not exist or is empty: " + videoPath);
            }
            log.info("Shipinhao upload start taskId={} accountKey={} video={} size={} title={}",
                    taskId, accountKey, videoPath, Files.size(videoPath), TextSupport.text(request.title()));
            String storageState = accountService.storageState(accountKey);
            log.info("Shipinhao upload storage state loaded taskId={} accountKey={} bytes={}", taskId, accountKey, storageState.getBytes(StandardCharsets.UTF_8).length);
            try (Browser browser = accountService.launchBrowser()) {
                BrowserContext context = accountService.newContext(browser, storageState);
                try {
                    Page page = context.newPage();
                    uploadVideoContent(page, request, videoPath, taskId);
                    accountService.saveStorageState(accountKey, context.storageState());
                } finally {
                    context.close();
                }
            }
            ShipinhaoAccountStatus account = accountService.status(accountKey);
            log.info("Shipinhao upload success taskId={} accountKey={} elapsedMs={}", taskId, accountKey, System.currentTimeMillis() - startedAt);
            return new ShipinhaoUploadResult(true, accountKey, account.nickname(), "上传成功", Map.of("manageUrl", ShipinhaoAccountService.MANAGE_URL));
        } catch (Exception exception) {
            log.error("Shipinhao upload failed taskId={} accountKey={} elapsedMs={} message={}",
                    taskId, accountKey, System.currentTimeMillis() - startedAt, exception.getMessage(), exception);
            if (exception instanceof IOException ioException) {
                throw ioException;
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Shipinhao upload failed", exception);
        } finally {
            diagnosticContext.remove();
            materialResolver.cleanupThrowing(resolvedVideo);
        }
    }

    private void uploadVideoContent(Page page, ShipinhaoUploadRequest request, Path videoPath, String taskId) {
        openCreateFromHome(page, taskId);
        dumpDiagnostics(page, taskId, "create-page-ready");

        Locator fileInput = page.locator("input[type='file'][accept*='video']").first();
        fileInput.waitFor(new Locator.WaitForOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED).setTimeout(30000));
        fileInput.setInputFiles(videoPath);
        page.waitForTimeout(8000);
        dumpDiagnostics(page, taskId, "after-set-video-file");

        fillDescription(page, request, taskId);
        setShortTitle(page, request, taskId);
        dumpDiagnostics(page, taskId, "after-fill-metadata");

        waitForUploadComplete(page, taskId);
        dumpDiagnostics(page, taskId, "upload-complete");

        if (Boolean.TRUE.equals(request.draft())) {
            clickSubmit(page, taskId, "保存草稿", "**/post/list**");
        } else {
            clickSubmit(page, taskId, "发表", "**/post/list**");
        }
        dumpDiagnostics(page, taskId, "submit-result");
    }

    private void openCreateFromHome(Page page, String taskId) {
        page.navigate(ShipinhaoAccountService.HOME_URL);
        page.waitForTimeout(5000);
        dumpDiagnostics(page, taskId, "home");
        String clicked = clickVisibleText(page, "发表视频");
        log.info("Shipinhao upload home publish click taskId={} method={}", taskId, clicked);
        if ("not-clicked".equals(clicked)) {
            Locator button = page.locator("button.weui-desktop-btn_primary").filter(new Locator.FilterOptions().setHasText("发表视频")).first();
            button.waitFor(new Locator.WaitForOptions().setTimeout(10000));
            button.click();
        }
        try {
            page.waitForURL("**/post/create**", new Page.WaitForURLOptions().setTimeout(20000));
        } catch (TimeoutError exception) {
            log.warn("Shipinhao upload did not navigate from home in time taskId={} url={}", taskId, page.url());
        }
        if (!page.url().contains("/post/create")) {
            page.navigate(ShipinhaoAccountService.PUBLISH_VIDEO_URL);
            page.waitForURL("**/post/create**", new Page.WaitForURLOptions().setTimeout(30000));
        }
        page.waitForTimeout(3000);
    }

    private void fillDescription(Page page, ShipinhaoUploadRequest request, String taskId) {
        String title = TextSupport.text(request.title());
        if (title.isBlank()) {
            throw new IllegalArgumentException("Missing title");
        }
        Locator editor = page.locator("div.input-editor").first();
        editor.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        editor.click();
        page.keyboard().type(title);
        page.keyboard().press("Enter");
        for (String tag : parseTags(request.tags())) {
            page.keyboard().type("#" + tag);
            page.keyboard().press("Space");
        }
        if (TextSupport.hasText(request.description())) {
            page.keyboard().press("Enter");
            page.keyboard().type(TextSupport.text(request.description()));
        }
        log.info("Shipinhao upload metadata filled taskId={} title={} tags={}", taskId, title, TextSupport.text(request.tags()));
    }

    private void setShortTitle(Page page, ShipinhaoUploadRequest request, String taskId) {
        try {
            String shortTitle = TextSupport.firstText(request.shortTitle(), formatShortTitle(request.title()));
            Locator input = page.getByText("短标题").first()
                    .locator("..")
                    .locator("xpath=following-sibling::div")
                    .locator("span input[type='text']")
                    .first();
            if (input.count() > 0) {
                input.fill(shortTitle);
                log.info("Shipinhao upload short title filled taskId={} shortTitle={}", taskId, shortTitle);
            }
        } catch (Exception exception) {
            log.warn("Shipinhao upload short title skipped taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void waitForUploadComplete(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(15).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                Locator publishButton = page.locator("div.form-btns button").filter(new Locator.FilterOptions().setHasText("发表")).first();
                if (publishButton.count() > 0) {
                    String className = TextSupport.text(publishButton.getAttribute("class"));
                    String disabled = publishButton.getAttribute("disabled");
                    if (checks == 1 || checks % 10 == 0) {
                        log.info("Shipinhao upload wait taskId={} checks={} class={} disabled={} url={}", taskId, checks, className, disabled, page.url());
                        dumpDiagnostics(page, taskId, "upload-wait-" + checks);
                    }
                    if (!className.contains("weui-desktop-btn_disabled") && disabled == null) {
                        return;
                    }
                }
                if (TextSupport.containsAny(PlaywrightDiagnostics.safeBodyText(page), "上传失败", "上传出错")) {
                    dumpDiagnostics(page, taskId, "upload-error");
                    throw new RuntimeException("Shipinhao video upload failed");
                }
            } catch (RuntimeException exception) {
                throw exception;
            } catch (Exception ignored) {
            }
            page.waitForTimeout(3000);
        }
        dumpDiagnostics(page, taskId, "upload-timeout");
        throw new RuntimeException("Timed out waiting for Shipinhao video upload to complete");
    }

    private void clickSubmit(Page page, String taskId, String buttonText, String successUrlPattern) {
        Locator button = page.locator("div.form-btns button").filter(new Locator.FilterOptions().setHasText(buttonText)).first();
        button.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        button.scrollIntoViewIfNeeded();
        page.waitForTimeout(500);
        button.click();
        dumpDiagnostics(page, taskId, "after-click-" + buttonText);
        try {
            page.waitForURL(successUrlPattern, new Page.WaitForURLOptions().setTimeout(30000));
        } catch (TimeoutError exception) {
            log.warn("Shipinhao upload submit did not reach list in time taskId={} button={} url={}", taskId, buttonText, page.url());
        }
        page.waitForTimeout(3000);
        if (!page.url().contains("/post/list")) {
            throw new RuntimeException("Shipinhao submit did not finish, currentUrl=" + page.url());
        }
    }

    private String clickVisibleText(Page page, String text) {
        Object result = page.evaluate(
                """
                (needle) => {
                  const isVisible = (el) => {
                    const style = getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                  };
                  const ownText = (el) => Array.from(el.childNodes)
                    .filter((node) => node.nodeType === Node.TEXT_NODE)
                    .map((node) => node.textContent || '')
                    .join('')
                    .trim();
                  const matches = Array.from(document.querySelectorAll('*'))
                    .filter((el) => isVisible(el) && (ownText(el) === needle || (el.innerText || el.textContent || '').trim() === needle));
                  matches.sort((a, b) => {
                    const ar = a.getBoundingClientRect();
                    const br = b.getBoundingClientRect();
                    const aButton = a.tagName === 'BUTTON' ? 0 : 1;
                    const bButton = b.tagName === 'BUTTON' ? 0 : 1;
                    if (aButton !== bButton) return aButton - bButton;
                    return (ar.width * ar.height) - (br.width * br.height);
                  });
                  const target = matches.find((el) => el.tagName === 'BUTTON') || matches[0];
                  if (!target) return 'not-clicked';
                  target.scrollIntoView({block: 'center', inline: 'center'});
                  const rect = target.getBoundingClientRect();
                  target.click();
                  return `${target.tagName}.${target.className || ''}:text=${(target.innerText || target.textContent || '').trim()}:rect=${Math.round(rect.x)},${Math.round(rect.y)},${Math.round(rect.width)},${Math.round(rect.height)}`;
                }
                """,
                text
        );
        return result == null ? "not-clicked" : result.toString();
    }

    private void dumpDiagnostics(Page page, String taskId, String label) {
        DiagnosticRunContext context = diagnosticContext.get();
        if (context == null) {
            context = new DiagnosticRunContext(taskId, taskId, DIAGNOSTIC_PLATFORM, DIAGNOSTIC_SOURCE, "");
        }
        diagnosticArtifactService.archive(new DiagnosticArtifactRequest(
                page,
                context.taskId(),
                context.runId(),
                context.platform(),
                context.source(),
                context.accountKey(),
                context.nextStepIndex(),
                label
        ));
    }

    private UploadMaterialResolver.ResolvedFile resolveVideo(ShipinhaoUploadRequest request) throws IOException {
        return materialResolver.resolveVideo(request.videoUrl(), request.minioUrl(), request.videoPath(), request.taskId());
    }

    private List<String> parseTags(String tags) {
        return Arrays.stream(TextSupport.text(tags).split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(value -> value.startsWith("#") ? value.substring(1) : value)
                .filter(value -> !value.isBlank())
                .toList();
    }

    private String formatShortTitle(String title) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < TextSupport.text(title).length(); i += 1) {
            char ch = TextSupport.text(title).charAt(i);
            if (Character.isLetterOrDigit(ch) || "《》“”:+?%°".indexOf(ch) >= 0) {
                builder.append(ch);
            }
        }
        String value = builder.toString();
        if (value.length() > 16) {
            value = value.substring(0, 16);
        }
        while (value.length() < 6) {
            value += " ";
        }
        return value;
    }
}
