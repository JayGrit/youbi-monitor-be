package com.youbi.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.BoundingBox;
import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class XiaohongshuUploadService {
    private static final Logger log = LoggerFactory.getLogger(XiaohongshuUploadService.class);
    private static final String PUBLISH_VIDEO_URL = XiaohongshuAccountService.PUBLISH_VIDEO_URL;
    private static final String SUCCESS_URL_PATTERN = "**/publish/success?**";
    private static final String DIAGNOSTIC_PLATFORM = "xiaohongshu";
    private static final String DIAGNOSTIC_SOURCE = "xiaohongshu-upload";

    private final XiaohongshuAccountService accountService;
    private final Path uploadWorkDir;
    private final UploadMaterialResolver materialResolver;
    private final SocialHumanActions humanActions;
    private final SocialRiskDetector riskDetector;
    private final DiagnosticArtifactService diagnosticArtifactService;
    private final ThreadLocal<DiagnosticRunContext> diagnosticContext = new ThreadLocal<>();

    public XiaohongshuUploadService(
            XiaohongshuAccountService accountService,
            SocialHumanActions humanActions,
            SocialRiskDetector riskDetector,
            DiagnosticArtifactService diagnosticArtifactService,
            ObjectMapper objectMapper,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir
    ) {
        this.accountService = accountService;
        this.humanActions = humanActions;
        this.riskDetector = riskDetector;
        this.diagnosticArtifactService = diagnosticArtifactService;
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        String bucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.uploadWorkDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).followRedirects(HttpClient.Redirect.NORMAL).build();
        this.materialResolver = new UploadMaterialResolver(minioClient, bucket, this.uploadWorkDir, httpClient, log, "XHS upload", true);
    }

    public XiaohongshuUploadResult upload(XiaohongshuUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        log.info("XHS upload start taskId={} accountKey={} videoPath={} videoUrl={} minioUrl={} title={}",
                taskId, accountKey, text(request.videoPath()), text(request.videoUrl()), text(request.minioUrl()), text(request.title()));
        UploadMaterialResolver.ResolvedFile resolvedVideo = resolveVideo(request);
        UploadMaterialResolver.ResolvedFile resolvedCover = resolveCover(request);
        DiagnosticRunContext diagnostics = new DiagnosticRunContext(taskId, taskId, DIAGNOSTIC_PLATFORM, DIAGNOSTIC_SOURCE, accountKey);
        diagnosticContext.set(diagnostics);
        try {
            Path videoPath = resolvedVideo.path();
            if (!Files.isRegularFile(videoPath) || Files.size(videoPath) == 0) {
                throw new IOException("Video file does not exist or is empty: " + videoPath);
            }
            if (resolvedCover != null && (!Files.isRegularFile(resolvedCover.path()) || Files.size(resolvedCover.path()) == 0)) {
                throw new IOException("Cover file does not exist or is empty: " + resolvedCover.path());
            }
            log.info("XHS upload material ready taskId={} video={} videoSizeBytes={} temporaryVideo={} cover={}",
                    taskId, videoPath, Files.size(videoPath), resolvedVideo.temporary(), resolvedCover == null ? "" : resolvedCover.path());

            String storageState = accountService.storageState(accountKey);
            log.info("XHS upload storage state loaded taskId={} accountKey={} bytes={}", taskId, accountKey, storageState.getBytes(StandardCharsets.UTF_8).length);
            try (Browser browser = accountService.launchBrowser()) {
                log.info("XHS upload browser launched taskId={} accountKey={}", taskId, accountKey);
                BrowserContext context = accountService.newContext(browser, storageState);
                try {
                    Page page = context.newPage();
                    uploadVideoContent(page, request, videoPath, resolvedCover == null ? null : resolvedCover.path(), taskId);
                    accountService.saveStorageState(accountKey, context.storageState());
                    log.info("XHS upload storage state saved taskId={} accountKey={}", taskId, accountKey);
                } finally {
                    context.close();
                }
            }
            XiaohongshuAccountStatus account = accountService.status(accountKey);
            log.info("XHS upload success taskId={} accountKey={} elapsedMs={}", taskId, accountKey, System.currentTimeMillis() - startedAt);
            return new XiaohongshuUploadResult(true, accountKey, account.nickname(), "上传成功", Map.of("successUrl", PUBLISH_VIDEO_URL));
        } catch (Exception exception) {
            log.error("XHS upload failed taskId={} accountKey={} elapsedMs={} message={}", taskId, accountKey, System.currentTimeMillis() - startedAt, exception.getMessage(), exception);
            if (exception instanceof IOException ioException) {
                throw ioException;
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Xiaohongshu upload failed", exception);
        } finally {
            diagnosticContext.remove();
            cleanup(resolvedVideo);
            cleanup(resolvedCover);
        }
    }

    private void uploadVideoContent(Page page, XiaohongshuUploadRequest request, Path videoPath, Path coverPath, String taskId) throws IOException {
        String title = truncate(required(request.title(), "title"), 20);
        log.info("XHS upload navigate publish page taskId={} url={}", taskId, PUBLISH_VIDEO_URL);
        page.navigate(PUBLISH_VIDEO_URL);
        page.waitForURL(PUBLISH_VIDEO_URL, new Page.WaitForURLOptions().setTimeout(30000));
        ensureNotBlocked(page, taskId, "open-publish-page");
        log.info("XHS upload set video input taskId={} file={}", taskId, videoPath);
        page.locator("div[class^='upload-content'] input[class='upload-input']").setInputFiles(videoPath);
        waitForVideoReady(page, taskId);

        Locator titleInput = page.locator("input[placeholder*='填写标题']").first();
        titleInput.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        log.info("XHS upload fill metadata taskId={} title={} tags={}", taskId, title, text(request.tags()));
        humanActions.fill(page, titleInput, title);
        fillDescriptionAndTags(page, request.description(), request.tags(), taskId);
        setCoverIfPresent(page, coverPath, taskId);
        setSchedule(page, request.schedule(), taskId);
        clickPublish(page, hasText(request.schedule()), taskId);
    }

    private void waitForVideoReady(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(15).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                Locator titleInput = page.locator("input[placeholder*='填写标题']").first();
                if (titleInput.count() > 0 && titleInput.isVisible()) {
                    log.info("XHS upload video editable taskId={} checks={} reason=title-input", taskId, checks);
                    return;
                }
                Locator preview = page.locator("input.upload-input").first()
                        .locator("xpath=following-sibling::div[contains(@class, 'preview-new')]");
                if (preview.count() > 0) {
                    String text = preview.innerText();
                    if (checks % 10 == 1) {
                        log.info("XHS upload waiting video taskId={} checks={} preview={}", taskId, checks, truncate(text.replace('\n', ' '), 200));
                    }
                    if (containsAny(text, "上传成功", "分辨率", "重新上传", "编辑封面", "已上传", "已选择", "100%")) {
                        log.info("XHS upload video editable taskId={} checks={} reason=preview text={}", taskId, checks, truncate(text.replace('\n', ' '), 200));
                        return;
                    }
                }
            } catch (Exception ignored) {
            }
            page.waitForTimeout(2000);
        }
        SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.XIAOHONGSHU, page);
        if (risk.blocking()) {
            dumpDiagnostics(page, taskId, "video-ready-risk-blocked");
            throw new RuntimeException("Xiaohongshu upload blocked while waiting video ready: " + risk.message());
        }
        throw new RuntimeException("Timed out waiting for Xiaohongshu video upload to become editable");
    }

    private void fillDescriptionAndTags(Page page, String description, String tags, String taskId) {
        Locator desc = page.locator("p[data-placeholder*='输入正文描述']").first();
        humanActions.click(page, desc);
        if (hasText(description)) {
            log.info("XHS upload fill description taskId={} chars={}", taskId, text(description).length());
            page.keyboard().press("Meta+A");
            page.keyboard().press("Backspace");
            humanActions.type(page, text(description));
            page.keyboard().press("Enter");
        }
        for (String tag : parseTags(tags)) {
            try {
                log.info("XHS upload fill tag taskId={} tag={}", taskId, tag);
                humanActions.type(page, "#" + tag);
                Locator topic = page.locator("#creator-editor-topic-container").first();
                topic.waitFor(new Locator.WaitForOptions().setTimeout(5000));
                Locator first = page.locator("#creator-editor-topic-container .item").first();
                first.waitFor(new Locator.WaitForOptions().setTimeout(3000));
                humanActions.click(page, first);
            } catch (Exception exception) {
                log.warn("XHS upload tag skipped taskId={} tag={} message={}", taskId, tag, exception.getMessage());
                page.keyboard().press("Enter");
            }
        }
    }

    private void setCoverIfPresent(Page page, Path coverPath, String taskId) {
        if (coverPath == null) {
            return;
        }
        try {
            setCover(page, coverPath, taskId);
        } catch (Exception exception) {
            log.warn("XHS upload cover skipped taskId={} cover={} message={}", taskId, coverPath, exception.getMessage());
            dumpDiagnostics(page, taskId, "cover-skipped");
            dismissCoverDialog(page, taskId);
        }
    }

    private void setCover(Page page, Path coverPath, String taskId) {
        log.info("XHS upload set cover taskId={} cover={}", taskId, coverPath);
        Locator coverDialog = page.locator("div.cover-plugin-title").filter(new Locator.FilterOptions().setHasText("设置封面"))
                .locator("xpath=ancestor::div[contains(@class, 'cover-plugin-preview')]")
                .locator("div.cover > div.default:visible");
        coverDialog.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        humanActions.click(page, coverDialog);
        Locator modal = page.locator("div.d-modal.cover-modal").first();
        modal.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        modal.locator("input[type='file'][accept*='image']").first().setInputFiles(coverPath);
        page.waitForTimeout(2000);
        humanActions.click(page, modal.locator("button.mojito-button").filter(new Locator.FilterOptions().setHasText("确定")).first());
        modal.waitFor(new Locator.WaitForOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.HIDDEN).setTimeout(30000));
    }

    private void dismissCoverDialog(Page page, String taskId) {
        try {
            Locator cancel = page.locator("button").filter(new Locator.FilterOptions().setHasText("取消")).last();
            if (cancel.count() > 0 && cancel.isVisible()) {
                humanActions.click(page, cancel);
                page.waitForTimeout(1000);
                log.info("XHS upload cover dialog dismissed taskId={} method=cancel", taskId);
                return;
            }
        } catch (Exception exception) {
            log.debug("XHS upload cover dialog cancel skipped taskId={} message={}", taskId, exception.getMessage());
        }
        try {
            page.keyboard().press("Escape");
            page.waitForTimeout(1000);
            log.info("XHS upload cover dialog dismissed taskId={} method=escape", taskId);
        } catch (Exception exception) {
            log.warn("XHS upload cover dialog dismiss failed taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void setSchedule(Page page, String schedule, String taskId) {
        if (!hasText(schedule)) {
            return;
        }
        log.info("XHS upload set schedule taskId={} schedule={}", taskId, text(schedule));
        humanActions.click(page, page.locator(".custom-switch-card").filter(new Locator.FilterOptions().setHasText("定时发布"))
                .locator(".d-switch").first());
        page.waitForTimeout(1000);
        humanActions.fill(page, page.locator(".d-datepicker-input-filter input.d-text").first(), text(schedule));
        page.waitForTimeout(1000);
    }

    private void clickPublish(Page page, boolean scheduled, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(30).toMillis();
        String buttonText = scheduled ? "定时发布" : "发布";
        RuntimeException last = null;
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            try {
                PageState state = pageState(page, buttonText);
                log.info("XHS upload publish wait taskId={} button={} attempt={} url={} uploading={} exactTextMatches={} visibleButtons={} exactTextElements={}",
                        taskId, buttonText, attempts, page.url(), state.uploading(), state.exactTextMatches(),
                        truncate(state.visibleButtons(), 500), truncate(state.exactTextElements(), 500));
                if (attempts == 1 || attempts % 30 == 0) {
                    dumpDiagnostics(page, taskId, "publish-wait-" + attempts);
                }
                if (state.uploading()) {
                    page.waitForTimeout(2000);
                    continue;
                }
                clickPublishButton(page, buttonText, taskId);
                page.waitForURL(SUCCESS_URL_PATTERN, new Page.WaitForURLOptions().setTimeout(5000));
                log.info("XHS upload publish success page reached taskId={} url={}", taskId, page.url());
                return;
            } catch (RuntimeException exception) {
                last = exception;
                if (attempts % 10 == 1) {
                    log.warn("XHS upload publish retry taskId={} attempt={} message={}", taskId, attempts, exception.getMessage());
                }
                page.waitForTimeout(2000);
            }
        }
        dumpDiagnostics(page, taskId, "publish-timeout");
        SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.XIAOHONGSHU, page);
        if (risk.blocking()) {
            throw new RuntimeException("Xiaohongshu publish blocked: " + risk.message());
        }
        throw last == null ? new RuntimeException("Timed out publishing Xiaohongshu video") : last;
    }

    private void ensureNotBlocked(Page page, String taskId, String label) {
        SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.XIAOHONGSHU, page);
        if (risk.blocking()) {
            dumpDiagnostics(page, taskId, label + "-risk-blocked");
            throw new RuntimeException("Xiaohongshu upload blocked: " + risk.message());
        }
    }

    private void clickPublishButton(Page page, String buttonText, String taskId) {
        Locator nativeButton = page.locator("button:has-text('" + buttonText + "'):visible");
        if (nativeButton.count() > 0) {
            humanActions.click(page, nativeButton.last());
            log.info("XHS upload publish click taskId={} method=native-button", taskId);
            return;
        }
        Locator publishComponent = page.locator("xhs-publish-btn[submit-disabled='false']").last();
        if (publishComponent.count() > 0) {
            publishComponent.scrollIntoViewIfNeeded();
            page.waitForTimeout(200);
            BoundingBox box = publishComponent.boundingBox();
            if (box != null) {
                double x = box.x + box.width - 220;
                double y = box.y + box.height / 2;
                page.mouse().click(x, y);
                log.info("XHS upload publish click taskId={} method=mouse-submit-area point={},{} rect={},{},{},{}",
                        taskId, Math.round(x), Math.round(y), Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height));
                return;
            }
        }
        String clicked = page.evaluate(
                """
                (buttonText) => {
                  const visible = (el) => {
                    if (!el) return false;
                    const rect = el.getBoundingClientRect();
                    const style = window.getComputedStyle(el);
                    return !!(rect.width && rect.height) && style.visibility !== 'hidden' && style.display !== 'none';
                  };
                  const clickElement = (el, method, point) => {
                    el.scrollIntoView({block: 'center', inline: 'center'});
                    const rect = el.getBoundingClientRect();
                    const x = point?.x ?? rect.left + rect.width / 2;
                    const y = point?.y ?? rect.top + rect.height / 2;
                    const target = document.elementFromPoint(x, y) || el;
                    for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
                      target.dispatchEvent(new MouseEvent(type, {bubbles: true, cancelable: true, view: window, clientX: x, clientY: y}));
                    }
                    return `${method}:target=${target.tagName}.${target.className || ''}:point=${Math.round(x)},${Math.round(y)}:rect=${Math.round(rect.x)},${Math.round(rect.y)},${Math.round(rect.width)},${Math.round(rect.height)}`;
                  };
                  const host = document.querySelector('xhs-publish-btn[submit-disabled="false"]');
                  if (host && host.shadowRoot) {
                    const elements = Array.from(host.shadowRoot.querySelectorAll('button, [role="button"], div, span'));
                    const target = elements.find((el) => visible(el) && (el.innerText || el.textContent || '').trim() === buttonText);
                    if (target) return clickElement(target, 'shadow-text');
                    const buttons = elements.filter((el) => visible(el) && (el.tagName === 'BUTTON' || el.getAttribute('role') === 'button'));
                    if (buttons.length) return clickElement(buttons[buttons.length - 1], 'shadow-last-button');
                  }
                  if (host && visible(host)) {
                    const rect = host.getBoundingClientRect();
                    return clickElement(host, 'publish-component-submit-area', {x: rect.right - 220, y: rect.top + rect.height / 2});
                  }
                  const textNodes = Array.from(document.querySelectorAll('*'))
                    .filter((el) => visible(el) && (el.innerText || el.textContent || '').trim() === buttonText);
                  if (textNodes.length) return clickElement(textNodes[textNodes.length - 1], 'dom-exact-text');
                  return 'not-clicked';
                }
                """,
                buttonText
        ).toString();
        log.info("XHS upload publish click taskId={} method={}", taskId, clicked);
        if (!"not-clicked".equals(clicked)) {
            return;
        }
        Locator textButton = page.locator("text=\"" + buttonText + "\"").last();
        humanActions.click(page, textButton);
        log.info("XHS upload publish click taskId={} method=playwright-text", taskId);
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

    private PageState pageState(Page page, String buttonText) {
        String body = PlaywrightDiagnostics.safeBodyText(page);
        return new PageState(
                containsAny(body, "视频上传中", "上传中", "智能推荐封面生成中"),
                PlaywrightDiagnostics.visibleButtonTexts(page),
                PlaywrightDiagnostics.exactTextElements(page, buttonText),
                PlaywrightDiagnostics.exactTextMatchCount(page, buttonText)
        );
    }

    private UploadMaterialResolver.ResolvedFile resolveVideo(XiaohongshuUploadRequest request) throws IOException {
        return materialResolver.resolveVideo(request.videoUrl(), request.minioUrl(), request.videoPath(), request.taskId());
    }

    private UploadMaterialResolver.ResolvedFile resolveCover(XiaohongshuUploadRequest request) throws IOException {
        return materialResolver.resolveCover(request.coverPath(), request.coverUrl(), request.taskId());
    }

    private List<String> parseTags(String tags) {
        return Arrays.stream(text(tags).split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .toList();
    }

    private boolean containsAny(String text, String... values) {
        return TextSupport.containsAny(text, values);
    }

    private void cleanup(UploadMaterialResolver.ResolvedFile file) throws IOException {
        materialResolver.cleanupThrowing(file);
    }

    private String truncate(String value, int max) {
        return TextSupport.truncate(value, max);
    }

    private String required(String value, String field) throws IOException {
        return TextSupport.required(value, field);
    }

    private boolean hasText(String value) {
        return TextSupport.hasText(value);
    }

    private String firstText(String... values) {
        return TextSupport.firstText(values);
    }

    private String text(String value) {
        return TextSupport.text(value);
    }

    private record PageState(boolean uploading, String visibleButtons, String exactTextElements, int exactTextMatches) {
    }
}
