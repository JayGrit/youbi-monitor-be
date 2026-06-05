package com.youbi.monitor.service;

import com.youbi.monitor.dto.KuaishouAccountStatus;
import com.youbi.monitor.dto.KuaishouUploadRequest;
import com.youbi.monitor.dto.KuaishouUploadResult;
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
public class KuaishouUploadService {
    private static final Logger log = LoggerFactory.getLogger(KuaishouUploadService.class);
    private static final String DIAGNOSTIC_PLATFORM = "kuaishou";
    private static final String DIAGNOSTIC_SOURCE = "kuaishou-upload";
    private static final List<String> UPLOAD_IN_PROGRESS_TEXTS = List.of(
            "上传中", "正在上传", "视频上传中", "处理中", "视频处理中", "转码中", "上传进度", "解析中"
    );
    private static final List<String> UPLOAD_FAILED_TEXTS = List.of(
            "上传失败", "上传出错", "上传异常"
    );
    private static final List<String> MISSING_VIDEO_TEXTS = List.of(
            "请上传视频", "请选择视频", "上传视频后", "请添加视频"
    );

    private final KuaishouAccountService accountService;
    private final UploadMaterialResolver materialResolver;
    private final DiagnosticArtifactService diagnosticArtifactService;
    private final UploaderAttemptService uploaderAttemptService;
    private final ThreadLocal<DiagnosticRunContext> diagnosticContext = new ThreadLocal<>();

    public KuaishouUploadService(
            KuaishouAccountService accountService,
            DiagnosticArtifactService diagnosticArtifactService,
            UploaderAttemptService uploaderAttemptService,
            AliDriveService aliDriveService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir
    ) {
        this.accountService = accountService;
        this.diagnosticArtifactService = diagnosticArtifactService;
        this.uploaderAttemptService = uploaderAttemptService;
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        String bucket = TextSupport.text(minioBucket).isBlank() ? "ydbi" : TextSupport.text(minioBucket);
        Path workDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).followRedirects(HttpClient.Redirect.NORMAL).build();
        this.materialResolver = new UploadMaterialResolver(minioClient, bucket, workDir, httpClient, aliDriveService, log, "Kuaishou upload", true);
    }

    public KuaishouUploadResult upload(KuaishouUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = TextSupport.firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        UploadMaterialResolver.ResolvedFile resolvedVideo = resolveVideo(request);
        String runId = uploaderAttemptService.nextRunId(taskId, DIAGNOSTIC_PLATFORM, accountKey);
        DiagnosticRunContext diagnostics = new DiagnosticRunContext(taskId, runId, DIAGNOSTIC_PLATFORM, DIAGNOSTIC_SOURCE, accountKey);
        diagnosticContext.set(diagnostics);
        try {
            Path videoPath = resolvedVideo.path();
            if (!Files.isRegularFile(videoPath) || Files.size(videoPath) == 0) {
                throw new IOException("Video file does not exist or is empty: " + videoPath);
            }
            log.info("Kuaishou upload start taskId={} accountKey={} video={} size={} title={}",
                    taskId, accountKey, videoPath, Files.size(videoPath), TextSupport.text(request.title()));
            String storageState = accountService.storageState(accountKey);
            log.info("Kuaishou upload storage state loaded taskId={} accountKey={} bytes={}", taskId, accountKey, storageState.getBytes(StandardCharsets.UTF_8).length);
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
            KuaishouAccountStatus account = accountService.status(accountKey);
            log.info("Kuaishou upload success taskId={} accountKey={} elapsedMs={}", taskId, accountKey, System.currentTimeMillis() - startedAt);
            return new KuaishouUploadResult(true, accountKey, account.nickname(), "上传成功", Map.of("manageUrl", KuaishouAccountService.MANAGE_URL));
        } catch (Exception exception) {
            log.error("Kuaishou upload failed taskId={} accountKey={} elapsedMs={} message={}",
                    taskId, accountKey, System.currentTimeMillis() - startedAt, exception.getMessage(), exception);
            if (exception instanceof IOException ioException) {
                throw ioException;
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Kuaishou upload failed", exception);
        } finally {
            diagnosticContext.remove();
            materialResolver.cleanupThrowing(resolvedVideo);
        }
    }

    private void uploadVideoContent(Page page, KuaishouUploadRequest request, Path videoPath, String taskId) {
        openPublishPage(page, taskId);
        dumpDiagnostics(page, taskId, "create-page-ready");

        Locator fileInput = page.locator("input[type='file']").first();
        fileInput.waitFor(new Locator.WaitForOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED).setTimeout(30000));
        fileInput.setInputFiles(videoPath);
        waitForVideoFileAccepted(page, taskId);
        dumpDiagnostics(page, taskId, "after-set-video-file");

        fillDescription(page, request, taskId);
        dumpDiagnostics(page, taskId, "after-fill-metadata");
        scrollToSubmitArea(page, taskId);

        waitForUploadComplete(page, taskId);
        dumpDiagnostics(page, taskId, "upload-complete");

        if (Boolean.TRUE.equals(request.draft())) {
            log.info("Kuaishou upload draft requested but current publish page has no draft action; publishing taskId={}", taskId);
        }
        clickSubmit(page, taskId, "发布", "**/article/**");
        dumpDiagnostics(page, taskId, "submit-result");
    }

    private void openPublishPage(Page page, String taskId) {
        page.navigate(KuaishouAccountService.PUBLISH_VIDEO_URL);
        page.waitForTimeout(5000);
        dumpDiagnostics(page, taskId, "publish-page");
        try {
            page.waitForURL("**/article/publish/video**", new Page.WaitForURLOptions().setTimeout(20000));
        } catch (TimeoutError exception) {
            log.warn("Kuaishou upload publish URL wait timed out taskId={} url={}", taskId, page.url());
        }
        if (hasVisibleLoginGate(page)) {
            dumpDiagnostics(page, taskId, "login-required");
            throw new RuntimeException("Kuaishou login state is expired or rejected by the publish page. url=" + page.url());
        }
    }

    private void fillDescription(Page page, KuaishouUploadRequest request, String taskId) {
        String title = TextSupport.text(request.title());
        if (title.isBlank()) {
            throw new IllegalArgumentException("Missing title");
        }
        String description = TextSupport.firstText(request.description(), title);
        StringBuilder caption = new StringBuilder(title);
        if (!description.equals(title)) {
            caption.append('\n').append(description);
        }
        for (String tag : parseTags(request.tags())) {
            caption.append(' ').append('#').append(tag);
        }
        fillFirstVisible(page, List.of(
                "textarea[placeholder*='描述']",
                "textarea[placeholder*='简介']",
                "textarea[placeholder*='文案']",
                "div[contenteditable='true']"
        ), caption.toString(), taskId, "description");
        log.info("Kuaishou upload metadata filled taskId={} title={} tags={}", taskId, title, TextSupport.text(request.tags()));
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
                    log.info("Kuaishou upload wait taskId={} checks={} ready={} stable={} buttonEnabled={} mediaVisible={} fileSelected={} uploading={} missingVideo={} uploadFailed={} url={} body={}",
                            taskId, checks, state.ready(), stableReadyChecks, state.buttonEnabled(), state.mediaVisible(), state.fileSelected(),
                            state.uploading(), state.missingVideo(), state.uploadFailed(), page.url(), TextSupport.truncate(state.body(), 300));
                    dumpDiagnostics(page, taskId, "upload-wait-" + checks);
                }
                if (state.uploadFailed()) {
                    dumpDiagnostics(page, taskId, "upload-error");
                    throw new RuntimeException("Kuaishou video upload failed");
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
        throw new RuntimeException("Timed out waiting for Kuaishou video upload to complete, lastState=" + lastState);
    }

    private void scrollToSubmitArea(Page page, String taskId) {
        try {
            Locator formButtons = page.getByText("发布", new Page.GetByTextOptions().setExact(true)).first();
            if (formButtons.count() > 0) {
                formButtons.scrollIntoViewIfNeeded();
                page.waitForTimeout(1000);
                log.info("Kuaishou upload scrolled to submit area taskId={} method=publish-button", taskId);
                return;
            }
        } catch (Exception exception) {
            log.debug("Kuaishou upload submit area scroll by locator skipped taskId={} message={}", taskId, exception.getMessage());
        }
        try {
            page.evaluate("() => window.scrollTo({top: document.body.scrollHeight, behavior: 'instant'})");
            page.waitForTimeout(1000);
            log.info("Kuaishou upload scrolled to submit area taskId={} method=window-bottom", taskId);
        } catch (Exception exception) {
            log.warn("Kuaishou upload submit area scroll failed taskId={} message={}", taskId, exception.getMessage());
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
                Locator button = page.getByText(buttonText, new Page.GetByTextOptions().setExact(true)).last();
                button.waitFor(new Locator.WaitForOptions().setTimeout(30000));
                button.scrollIntoViewIfNeeded();
                page.waitForTimeout(500);
                String clicked = clickVisibleText(page, buttonText);
                if ("not-clicked".equals(clicked)) {
                    button.click(new Locator.ClickOptions().setForce(true));
                }
                dumpDiagnostics(page, taskId, "after-click-" + buttonText + "-" + attempts);
                try {
                    page.waitForURL(successUrlPattern, new Page.WaitForURLOptions().setTimeout(30000));
                } catch (TimeoutError exception) {
                    log.warn("Kuaishou upload submit did not reach list in time taskId={} button={} attempt={} url={}", taskId, buttonText, attempts, page.url());
                }
                page.waitForTimeout(3000);
                if (!page.url().contains("/article/publish/video") || TextSupport.containsAny(PlaywrightDiagnostics.safeBodyText(page), "发布成功", "作品管理", "审核中")) {
                    return;
                }
                String body = PlaywrightDiagnostics.safeBodyText(page);
                if (TextSupport.containsAny(body, MISSING_VIDEO_TEXTS.toArray(String[]::new))) {
                    dumpDiagnostics(page, taskId, "submit-missing-video-" + attempts);
                    log.warn("Kuaishou upload submit reported missing video taskId={} attempt={} body={}",
                            taskId, attempts, TextSupport.truncate(body, 300));
                    page.waitForTimeout(5000);
                    continue;
                }
                last = new RuntimeException("Kuaishou submit did not finish, currentUrl=" + page.url());
            } catch (RuntimeException exception) {
                last = exception;
                log.warn("Kuaishou upload submit retry taskId={} button={} attempt={} message={}",
                        taskId, buttonText, attempts, exception.getMessage());
                page.waitForTimeout(3000);
            }
        }
        dumpDiagnostics(page, taskId, "submit-failed");
        throw last == null ? new RuntimeException("Kuaishou submit did not finish") : last;
    }

    private void waitForVideoFileAccepted(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                UploadReadiness state = uploadReadiness(page);
                if (state.fileSelected() || state.mediaVisible()) {
                    log.info("Kuaishou upload video file accepted taskId={} checks={} fileSelected={} mediaVisible={}",
                            taskId, checks, state.fileSelected(), state.mediaVisible());
                    return;
                }
            } catch (Exception ignored) {
            }
            page.waitForTimeout(1000);
        }
        dumpDiagnostics(page, taskId, "video-file-not-accepted");
        log.warn("Kuaishou upload selected video was not detected by early DOM check taskId={}, continue to metadata/upload readiness checks", taskId);
    }

    private UploadReadiness uploadReadiness(Page page) {
        String body = PlaywrightDiagnostics.safeBodyText(page);
        boolean buttonEnabled = body.contains("发布") && !body.contains("上传中") && !body.contains("视频处理中");
        boolean fileSelected = Boolean.TRUE.equals(page.evaluate(
                """
                () => Array.from(document.querySelectorAll('input[type="file"]'))
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
        boolean uploadPreviewText = body.contains("封面")
                || body.contains("重新上传")
                || body.contains("删除")
                || body.contains("视频信息");
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

    private void fillFirstVisible(Page page, List<String> selectors, String value, String taskId, String label) {
        RuntimeException last = null;
        for (String selector : selectors) {
            try {
                Locator locator = page.locator(selector).first();
                if (locator.count() == 0) {
                    continue;
                }
                locator.waitFor(new Locator.WaitForOptions().setTimeout(3000));
                locator.scrollIntoViewIfNeeded();
                locator.click(new Locator.ClickOptions().setForce(true));
                page.keyboard().press("Control+A");
                page.keyboard().press("Delete");
                page.keyboard().type(value);
                log.info("Kuaishou upload filled {} taskId={} selector={}", label, taskId, selector);
                return;
            } catch (RuntimeException exception) {
                last = exception;
            }
        }
        dumpDiagnostics(page, taskId, "fill-" + label + "-failed");
        throw last == null ? new RuntimeException("Kuaishou " + label + " field not found") : last;
    }

    private boolean hasVisibleLoginGate(Page page) {
        String body = PlaywrightDiagnostics.safeBodyText(page);
        return TextSupport.containsAny(body, "扫码登录", "登录/注册", "请使用快手扫码", "手机号登录")
                && !TextSupport.containsAny(body, "发布视频", "上传视频", "作品管理");
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

    private UploadMaterialResolver.ResolvedFile resolveVideo(KuaishouUploadRequest request) throws IOException {
        return materialResolver.resolveVideo(request.videoLocation(), request.videoUrl(), request.minioUrl(), request.videoPath(), request.alidriveFileId(), request.alidriveRemotePath(), request.taskId());
    }

    private List<String> parseTags(String tags) {
        return Arrays.stream(TextSupport.text(tags).split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(value -> value.startsWith("#") ? value.substring(1) : value)
                .filter(value -> !value.isBlank())
                .toList();
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
