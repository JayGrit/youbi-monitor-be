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
    private static final List<String> UPLOAD_IN_PROGRESS_TEXTS = List.of(
            "上传中", "正在上传", "视频上传中", "处理中", "视频处理中", "转码中", "上传进度"
    );
    private static final List<String> UPLOAD_FAILED_TEXTS = List.of(
            "上传失败", "上传出错", "上传异常"
    );
    private static final List<String> MISSING_VIDEO_TEXTS = List.of(
            "请上传视频", "请选择视频", "上传视频后"
    );

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
        waitForVideoFileAccepted(page, taskId);
        dumpDiagnostics(page, taskId, "after-set-video-file");

        fillDescription(page, request, taskId);
        setShortTitle(page, request, taskId);
        dumpDiagnostics(page, taskId, "after-fill-metadata");
        scrollToSubmitArea(page, taskId);

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
        int stableReadyChecks = 0;
        UploadReadiness lastState = null;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                UploadReadiness state = uploadReadiness(page);
                lastState = state;
                if (checks == 1 || checks % 10 == 0 || state.ready()) {
                    log.info("Shipinhao upload wait taskId={} checks={} ready={} stable={} buttonEnabled={} mediaVisible={} fileSelected={} uploading={} missingVideo={} uploadFailed={} url={} body={}",
                            taskId, checks, state.ready(), stableReadyChecks, state.buttonEnabled(), state.mediaVisible(), state.fileSelected(),
                            state.uploading(), state.missingVideo(), state.uploadFailed(), page.url(), TextSupport.truncate(state.body(), 300));
                    dumpDiagnostics(page, taskId, "upload-wait-" + checks);
                }
                if (state.uploadFailed()) {
                    dumpDiagnostics(page, taskId, "upload-error");
                    throw new RuntimeException("Shipinhao video upload failed");
                }
                if (state.mediaVisible() && !state.buttonEnabled() && checks % 3 == 0) {
                    scrollToSubmitArea(page, taskId);
                }
                if (state.ready()) {
                    stableReadyChecks += 1;
                    if (stableReadyChecks >= 2) {
                        return;
                    }
                } else {
                    stableReadyChecks = 0;
                }
            } catch (RuntimeException exception) {
                throw exception;
            } catch (Exception ignored) {
            }
            page.waitForTimeout(3000);
        }
        dumpDiagnostics(page, taskId, "upload-timeout");
        throw new RuntimeException("Timed out waiting for Shipinhao video upload to complete, lastState=" + lastState);
    }

    private void scrollToSubmitArea(Page page, String taskId) {
        try {
            Locator formButtons = page.locator("div.form-btns").first();
            if (formButtons.count() > 0) {
                formButtons.scrollIntoViewIfNeeded();
                page.waitForTimeout(1000);
                log.info("Shipinhao upload scrolled to submit area taskId={} method=form-buttons", taskId);
                return;
            }
        } catch (Exception exception) {
            log.debug("Shipinhao upload submit area scroll by locator skipped taskId={} message={}", taskId, exception.getMessage());
        }
        try {
            page.evaluate("() => window.scrollTo({top: document.body.scrollHeight, behavior: 'instant'})");
            page.waitForTimeout(1000);
            log.info("Shipinhao upload scrolled to submit area taskId={} method=window-bottom", taskId);
        } catch (Exception exception) {
            log.warn("Shipinhao upload submit area scroll failed taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void clickSubmit(Page page, String taskId, String buttonText, String successUrlPattern) {
        RuntimeException last = null;
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(6).toMillis();
        int attempts = 0;
        while (System.currentTimeMillis() < deadline && attempts < 3) {
            attempts += 1;
            waitForUploadComplete(page, taskId);
            try {
                Locator button = page.locator("div.form-btns button").filter(new Locator.FilterOptions().setHasText(buttonText)).first();
                button.waitFor(new Locator.WaitForOptions().setTimeout(30000));
                button.scrollIntoViewIfNeeded();
                page.waitForTimeout(500);
                button.click();
                dumpDiagnostics(page, taskId, "after-click-" + buttonText + "-" + attempts);
                try {
                    page.waitForURL(successUrlPattern, new Page.WaitForURLOptions().setTimeout(30000));
                } catch (TimeoutError exception) {
                    log.warn("Shipinhao upload submit did not reach list in time taskId={} button={} attempt={} url={}", taskId, buttonText, attempts, page.url());
                }
                page.waitForTimeout(3000);
                if (page.url().contains("/post/list")) {
                    return;
                }
                String body = PlaywrightDiagnostics.safeBodyText(page);
                if (TextSupport.containsAny(body, MISSING_VIDEO_TEXTS.toArray(String[]::new))) {
                    dumpDiagnostics(page, taskId, "submit-missing-video-" + attempts);
                    log.warn("Shipinhao upload submit reported missing video taskId={} attempt={} body={}",
                            taskId, attempts, TextSupport.truncate(body, 300));
                    page.waitForTimeout(5000);
                    continue;
                }
                last = new RuntimeException("Shipinhao submit did not finish, currentUrl=" + page.url());
            } catch (RuntimeException exception) {
                last = exception;
                log.warn("Shipinhao upload submit retry taskId={} button={} attempt={} message={}",
                        taskId, buttonText, attempts, exception.getMessage());
                page.waitForTimeout(3000);
            }
        }
        dumpDiagnostics(page, taskId, "submit-failed");
        throw last == null ? new RuntimeException("Shipinhao submit did not finish") : last;
    }

    private void waitForVideoFileAccepted(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                UploadReadiness state = uploadReadiness(page);
                if (state.fileSelected() || state.mediaVisible()) {
                    log.info("Shipinhao upload video file accepted taskId={} checks={} fileSelected={} mediaVisible={}",
                            taskId, checks, state.fileSelected(), state.mediaVisible());
                    return;
                }
            } catch (Exception ignored) {
            }
            page.waitForTimeout(1000);
        }
        dumpDiagnostics(page, taskId, "video-file-not-accepted");
        log.warn("Shipinhao upload selected video was not detected by early DOM check taskId={}, continue to metadata/upload readiness checks", taskId);
    }

    private UploadReadiness uploadReadiness(Page page) {
        String body = PlaywrightDiagnostics.safeBodyText(page);
        Locator publishButton = page.locator("div.form-btns button").filter(new Locator.FilterOptions().setHasText("发表")).first();
        boolean buttonEnabled = false;
        if (publishButton.count() > 0) {
            String className = TextSupport.text(publishButton.getAttribute("class"));
            String disabled = publishButton.getAttribute("disabled");
            buttonEnabled = !className.contains("weui-desktop-btn_disabled") && disabled == null && publishButton.isEnabled();
        }
        boolean fileSelected = Boolean.TRUE.equals(page.evaluate(
                """
                () => Array.from(document.querySelectorAll('input[type="file"][accept*="video"]'))
                  .some((input) => input.files && input.files.length > 0)
                """
        ));
        boolean domMediaVisible = Boolean.TRUE.equals(page.evaluate(
                """
                () => {
                  const visible = (el) => {
                    const rect = el.getBoundingClientRect();
                    const style = getComputedStyle(el);
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width >= 80 && rect.height >= 60;
                  };
                  return Array.from(document.querySelectorAll('video, canvas, img')).some((el) => {
                    if (!visible(el)) return false;
                    const src = el.getAttribute('src') || '';
                    const cls = el.className || '';
                    const text = (el.closest('form, .post-create, .weui-desktop-form, .form') || document.body).innerText || '';
                    return src.startsWith('blob:') || src.startsWith('data:') || /cover|poster|preview|thumb|video/i.test(cls) || text.includes('封面');
                  });
                }
                """
        ));
        boolean uploadPreviewText = body.contains("封面预览")
                || (body.contains("个人主页卡片") && body.contains("分享卡片"))
                || (body.contains("删除") && body.contains("短标题"));
        boolean mediaVisible = domMediaVisible || uploadPreviewText;
        boolean uploading = TextSupport.containsAny(body, UPLOAD_IN_PROGRESS_TEXTS.toArray(String[]::new));
        boolean uploadFailed = TextSupport.containsAny(body, UPLOAD_FAILED_TEXTS.toArray(String[]::new));
        boolean missingVideo = TextSupport.containsAny(body, MISSING_VIDEO_TEXTS.toArray(String[]::new));
        boolean pageStateReadable = TextSupport.hasText(body);
        boolean ready = buttonEnabled && !uploading && !uploadFailed && !missingVideo && (mediaVisible || !pageStateReadable);
        return new UploadReadiness(ready, buttonEnabled, mediaVisible, fileSelected, uploading, uploadFailed, missingVideo, body);
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

    private record UploadReadiness(
            boolean ready,
            boolean buttonEnabled,
            boolean mediaVisible,
            boolean fileSelected,
            boolean uploading,
            boolean uploadFailed,
            boolean missingVideo,
            String body
    ) {
        @Override
        public String toString() {
            return "UploadReadiness{"
                    + "ready=" + ready
                    + ", buttonEnabled=" + buttonEnabled
                    + ", mediaVisible=" + mediaVisible
                    + ", fileSelected=" + fileSelected
                    + ", uploading=" + uploading
                    + ", uploadFailed=" + uploadFailed
                    + ", missingVideo=" + missingVideo
                    + ", body='" + TextSupport.truncate(body, 200) + "'"
                    + '}';
        }
    }
}
