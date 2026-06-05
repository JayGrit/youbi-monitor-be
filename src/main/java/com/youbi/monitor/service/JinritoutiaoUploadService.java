package com.youbi.monitor.service;

import com.youbi.monitor.dto.JinritoutiaoAccountStatus;
import com.youbi.monitor.dto.JinritoutiaoUploadRequest;
import com.youbi.monitor.dto.JinritoutiaoUploadResult;
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

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
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
public class JinritoutiaoUploadService {
    private static final Logger log = LoggerFactory.getLogger(JinritoutiaoUploadService.class);
    private static final String DIAGNOSTIC_PLATFORM = "jinritoutiao";
    private static final String DIAGNOSTIC_SOURCE = "jinritoutiao-upload";
    private static final int MIN_COVER_WIDTH = 412;
    private static final int MIN_COVER_HEIGHT = 667;
    private static final List<String> UPLOAD_IN_PROGRESS_TEXTS = List.of(
            "上传中", "正在上传", "视频上传中", "处理中", "视频处理中", "转码中", "上传进度", "解析中"
    );
    private static final List<String> UPLOAD_FAILED_TEXTS = List.of(
            "上传失败", "上传出错", "上传异常"
    );
    private static final List<String> MISSING_VIDEO_TEXTS = List.of(
            "请上传视频", "请选择视频", "上传视频后", "请添加视频"
    );
    private static final List<String> TITLE_SELECTORS = List.of(
            ".form-item-title input",
            "input[placeholder*='0～30']",
            "input[placeholder*='0-30']",
            "input[placeholder*='1～30']",
            "input[placeholder*='1-30']",
            "input[placeholder*='请输入 0']",
            "input[placeholder*='请输入 1']",
            "input.xigua-input.show-limit"
    );
    private static final List<String> DESCRIPTION_SELECTORS = List.of(
            ".form-item-abstract textarea",
            "textarea[placeholder*='视频简介']",
            "textarea[placeholder*='简介']"
    );

    private final JinritoutiaoAccountService accountService;
    private final UploadMaterialResolver materialResolver;
    private final DiagnosticArtifactService diagnosticArtifactService;
    private final UploaderAttemptService uploaderAttemptService;
    private final ThreadLocal<DiagnosticRunContext> diagnosticContext = new ThreadLocal<>();

    public JinritoutiaoUploadService(
            JinritoutiaoAccountService accountService,
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
        this.materialResolver = new UploadMaterialResolver(minioClient, bucket, workDir, httpClient, aliDriveService, log, "Jinritoutiao upload", true);
    }

    public JinritoutiaoUploadResult upload(JinritoutiaoUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = TextSupport.firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        UploadMaterialResolver.ResolvedFile resolvedVideo = resolveVideo(request);
        UploadMaterialResolver.ResolvedFile resolvedCover = resolveCover(request);
        String runId = uploaderAttemptService.nextRunId(taskId, DIAGNOSTIC_PLATFORM, accountKey);
        DiagnosticRunContext diagnostics = new DiagnosticRunContext(taskId, runId, DIAGNOSTIC_PLATFORM, DIAGNOSTIC_SOURCE, accountKey);
        diagnosticContext.set(diagnostics);
        try {
            Path videoPath = resolvedVideo.path();
            if (!Files.isRegularFile(videoPath) || Files.size(videoPath) == 0) {
                throw new IOException("Video file does not exist or is empty: " + videoPath);
            }
            if (resolvedCover != null && (!Files.isRegularFile(resolvedCover.path()) || Files.size(resolvedCover.path()) == 0)) {
                throw new IOException("Cover file does not exist or is empty: " + resolvedCover.path());
            }
            log.info("Jinritoutiao upload start taskId={} accountKey={} video={} size={} title={}",
                    taskId, accountKey, videoPath, Files.size(videoPath), TextSupport.text(request.title()));
            String storageState = accountService.storageState(accountKey);
            log.info("Jinritoutiao upload storage state loaded taskId={} accountKey={} bytes={}", taskId, accountKey, storageState.getBytes(StandardCharsets.UTF_8).length);
            try (Browser browser = accountService.launchBrowser()) {
                BrowserContext context = accountService.newContext(browser, storageState);
                try {
                    Page page = context.newPage();
                    uploadVideoContent(page, request, videoPath, resolvedCover == null ? null : resolvedCover.path(), taskId);
                    accountService.saveStorageState(accountKey, context.storageState());
                } finally {
                    context.close();
                }
            }
            JinritoutiaoAccountStatus account = accountService.status(accountKey);
            log.info("Jinritoutiao upload success taskId={} accountKey={} elapsedMs={}", taskId, accountKey, System.currentTimeMillis() - startedAt);
            return new JinritoutiaoUploadResult(true, accountKey, account.nickname(), "上传成功", Map.of("manageUrl", JinritoutiaoAccountService.MANAGE_URL));
        } catch (Exception exception) {
            log.error("Jinritoutiao upload failed taskId={} accountKey={} elapsedMs={} message={}",
                    taskId, accountKey, System.currentTimeMillis() - startedAt, exception.getMessage(), exception);
            if (exception instanceof IOException ioException) {
                throw ioException;
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Jinritoutiao upload failed", exception);
        } finally {
            diagnosticContext.remove();
            materialResolver.cleanupThrowing(resolvedVideo);
            materialResolver.cleanupThrowing(resolvedCover);
        }
    }

    private void uploadVideoContent(Page page, JinritoutiaoUploadRequest request, Path videoPath, Path coverPath, String taskId) {
        openPublishPage(page, taskId);
        dumpDiagnostics(page, taskId, "create-page-ready");

        Locator fileInput = page.locator("input[type='file']").first();
        fileInput.waitFor(new Locator.WaitForOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED).setTimeout(30000));
        fileInput.setInputFiles(videoPath);
        waitForVideoFileAccepted(page, taskId);
        waitForMetadataForm(page, taskId);
        dumpDiagnostics(page, taskId, "after-set-video-file");

        dismissKnownDialogs(page, taskId);
        fillTitleTopicsAndDescription(page, request, taskId);
        dumpDiagnostics(page, taskId, "after-fill-metadata");
        scrollToSubmitArea(page, taskId);

        waitForUploadComplete(page, taskId);
        if (coverPath == null) {
            selectGeneratedCover(page, taskId);
        } else {
            uploadCover(page, coverPath, taskId);
        }
        dismissKnownDialogs(page, taskId);
        dumpDiagnostics(page, taskId, "upload-complete");

        if (Boolean.TRUE.equals(request.draft())) {
            clickSubmit(page, taskId, "存草稿", "**/profile_v4/xigua/**");
        } else {
            clickSubmit(page, taskId, "发布", "**/profile_v4/xigua/**");
        }
        dumpDiagnostics(page, taskId, "submit-result");
    }

    private void openPublishPage(Page page, String taskId) {
        page.navigate(JinritoutiaoAccountService.PUBLISH_VIDEO_URL);
        page.waitForTimeout(5000);
        dumpDiagnostics(page, taskId, "publish-page");
        try {
            page.waitForURL("**/profile_v4/xigua/upload-video**", new Page.WaitForURLOptions().setTimeout(20000));
        } catch (TimeoutError exception) {
            log.warn("Jinritoutiao upload publish URL wait timed out taskId={} url={}", taskId, page.url());
        }
        if (hasVisibleLoginGate(page)) {
            dumpDiagnostics(page, taskId, "login-required");
            throw new RuntimeException("Jinritoutiao login state is expired or rejected by the publish page. url=" + page.url());
        }
    }

    private void fillTitleTopicsAndDescription(Page page, JinritoutiaoUploadRequest request, String taskId) {
        String title = TextSupport.truncate(TextSupport.text(request.title()), 30);
        if (title.isBlank()) {
            throw new IllegalArgumentException("Missing title");
        }
        String description = TextSupport.firstText(request.description(), title);
        fillFirstVisible(page, TITLE_SELECTORS, title, taskId, "title");
        fillFirstVisibleOptional(page, DESCRIPTION_SELECTORS, description, taskId, "description");
        for (String tag : parseTags(request.tags())) {
            fillTopic(page, tag, taskId);
        }
        log.info("Jinritoutiao upload metadata filled taskId={} title={} tags={}", taskId, title, TextSupport.text(request.tags()));
    }

    private void fillTopic(Page page, String tag, String taskId) {
        try {
            Locator input = page.locator("input.arco-input-tag-input, input[placeholder='请输入']").last();
            input.waitFor(new Locator.WaitForOptions().setTimeout(3000));
            input.scrollIntoViewIfNeeded();
            input.click(new Locator.ClickOptions().setForce(true));
            page.keyboard().type(tag);
            page.keyboard().press("Enter");
            log.info("Jinritoutiao upload topic filled taskId={} tag={}", taskId, tag);
        } catch (Exception exception) {
            log.warn("Jinritoutiao upload topic skipped taskId={} tag={} message={}", taskId, tag, exception.getMessage());
        }
    }

    private void dismissKnownDialogs(Page page, String taskId) {
        for (String text : List.of("我知道了", "知道了", "已知悉协议")) {
            try {
                String clicked = clickVisibleText(page, text);
                if (!"not-clicked".equals(clicked)) {
                    log.info("Jinritoutiao upload dismissed dialog taskId={} text={} method={}", taskId, text, clicked);
                    page.waitForTimeout(1000);
                    return;
                }
            } catch (Exception ignored) {
            }
        }
    }

    private void uploadCover(Page page, Path coverPath, String taskId) {
        Path preparedCoverPath = coverPath;
        try {
            if (TextSupport.containsAny(PlaywrightDiagnostics.safeBodyText(page), "修改封面", "重新上传封面")) {
                return;
            }
            preparedCoverPath = prepareCoverForUpload(coverPath, taskId);
            String clicked = clickVisibleText(page, "上传封面");
            if ("not-clicked".equals(clicked)) {
                log.warn("Jinritoutiao upload cover trigger not found taskId={}", taskId);
                return;
            }
            page.waitForTimeout(1500);
            clickVisibleText(page, "本地上传");
            Locator coverInput = page.locator("input[type='file'][accept*='image']").last();
            coverInput.waitFor(new Locator.WaitForOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED).setTimeout(30000));
            coverInput.setInputFiles(preparedCoverPath);
            page.waitForTimeout(3000);
            dumpDiagnostics(page, taskId, "cover-uploaded");
            confirmCoverEditor(page, taskId);
            dumpDiagnostics(page, taskId, "cover-confirmed");
            log.info("Jinritoutiao upload cover uploaded taskId={} cover={} preparedCover={} trigger={}", taskId, coverPath, preparedCoverPath, clicked);
        } catch (Exception exception) {
            dumpDiagnostics(page, taskId, "cover-upload-failed");
            log.warn("Jinritoutiao upload cover upload failed taskId={} cover={} message={}", taskId, coverPath, exception.getMessage());
            selectGeneratedCover(page, taskId);
        } finally {
            if (preparedCoverPath != null && !preparedCoverPath.equals(coverPath)) {
                try {
                    Files.deleteIfExists(preparedCoverPath);
                } catch (IOException exception) {
                    log.warn("Jinritoutiao upload temporary cover cleanup failed taskId={} cover={} message={}",
                            taskId, preparedCoverPath, exception.getMessage());
                }
            }
        }
    }

    private Path prepareCoverForUpload(Path coverPath, String taskId) throws IOException {
        BufferedImage source = ImageIO.read(coverPath.toFile());
        if (source == null) {
            log.warn("Jinritoutiao upload cover image cannot be decoded taskId={} cover={}, using original", taskId, coverPath);
            return coverPath;
        }
        int width = source.getWidth();
        int height = source.getHeight();
        if (width >= MIN_COVER_WIDTH && height >= MIN_COVER_HEIGHT) {
            return coverPath;
        }
        double scale = Math.max((double) MIN_COVER_WIDTH / width, (double) MIN_COVER_HEIGHT / height);
        int targetWidth = Math.max(MIN_COVER_WIDTH, (int) Math.ceil(width * scale));
        int targetHeight = Math.max(MIN_COVER_HEIGHT, (int) Math.ceil(height * scale));
        BufferedImage target = new BufferedImage(targetWidth, targetHeight, BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = target.createGraphics();
        try {
            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, targetWidth, targetHeight);
            graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            graphics.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            graphics.drawImage(source, 0, 0, targetWidth, targetHeight, null);
        } finally {
            graphics.dispose();
        }
        Path prepared = Files.createTempFile("jinritoutiao-cover-", ".jpg");
        if (!ImageIO.write(target, "jpg", prepared.toFile())) {
            throw new IOException("No JPEG writer available for Jinritoutiao cover");
        }
        log.info("Jinritoutiao upload cover upscaled taskId={} cover={} original={}x{} prepared={} preparedSize={}x{}",
                taskId, coverPath, width, height, prepared, targetWidth, targetHeight);
        return prepared;
    }

    private void selectGeneratedCover(Page page, String taskId) {
        try {
            if (TextSupport.containsAny(PlaywrightDiagnostics.safeBodyText(page), "修改封面", "重新上传封面")) {
                return;
            }
            String clicked = clickVisibleText(page, "上传封面");
            if ("not-clicked".equals(clicked)) {
                log.warn("Jinritoutiao upload cover trigger not found taskId={}", taskId);
                return;
            }
            page.waitForTimeout(2000);
            dumpDiagnostics(page, taskId, "cover-picker-open");

            Locator firstFrame = page.locator("img.select-img").first();
            if (firstFrame.count() > 0) {
                firstFrame.click(new Locator.ClickOptions().setForce(true));
                page.waitForTimeout(500);
            }
            clickVisibleText(page, "下一步");
            page.waitForTimeout(2000);
            confirmCoverEditor(page, taskId);
            dumpDiagnostics(page, taskId, "cover-selected");
            log.info("Jinritoutiao upload selected generated cover taskId={} trigger={}", taskId, clicked);
        } catch (Exception exception) {
            dumpDiagnostics(page, taskId, "cover-select-failed");
            log.warn("Jinritoutiao upload generated cover selection skipped taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void confirmCoverEditor(Page page, String taskId) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= 4; attempt++) {
            String body = PlaywrightDiagnostics.safeBodyText(page);
            if (!coverEditorNeedsConfirmation(body)) {
                page.waitForTimeout(1000);
                if (!coverEditorNeedsConfirmation(PlaywrightDiagnostics.safeBodyText(page))) {
                    log.info("Jinritoutiao upload cover editor closed taskId={} attempts={}", taskId, attempt - 1);
                    return;
                }
            }
            for (String text : List.of("完成", "确定", "确认")) {
                try {
                    String clicked = clickTopmostVisibleText(page, text);
                    if (!"not-clicked".equals(clicked)) {
                        log.info("Jinritoutiao upload cover editor confirm clicked taskId={} attempt={} text={} method={}",
                                taskId, attempt, text, clicked);
                        page.waitForTimeout(2000);
                        dumpDiagnostics(page, taskId, "cover-confirm-click-" + attempt);
                        break;
                    }
                } catch (RuntimeException exception) {
                    last = exception;
                }
            }
        }
        String body = PlaywrightDiagnostics.safeBodyText(page);
        if (coverEditorNeedsConfirmation(body)) {
            dumpDiagnostics(page, taskId, "cover-confirm-timeout");
            throw last == null
                    ? new RuntimeException("Jinritoutiao cover editor did not close, body=" + TextSupport.truncate(body, 200))
                    : last;
        }
    }

    private boolean coverEditorNeedsConfirmation(String body) {
        return TextSupport.containsAny(body,
                "封面编辑",
                "完成后无法继续编辑",
                "是否确定完成",
                "图片截取",
                "本地上传"
        );
    }

    private String clickTopmostVisibleText(Page page, String text) {
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
                  const isTopmost = (el) => {
                    const rect = el.getBoundingClientRect();
                    const x = Math.min(Math.max(rect.left + rect.width / 2, 0), window.innerWidth - 1);
                    const y = Math.min(Math.max(rect.top + rect.height / 2, 0), window.innerHeight - 1);
                    const top = document.elementFromPoint(x, y);
                    return top && (el === top || el.contains(top));
                  };
                  const matches = Array.from(document.querySelectorAll('*'))
                    .filter((el) => isVisible(el) && isTopmost(el) && (ownText(el) === needle || (el.innerText || el.textContent || '').trim() === needle));
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
                    log.info("Jinritoutiao upload wait taskId={} checks={} ready={} stable={} buttonEnabled={} mediaVisible={} fileSelected={} uploading={} missingVideo={} uploadFailed={} url={} body={}",
                            taskId, checks, state.ready(), stableReadyChecks, state.buttonEnabled(), state.mediaVisible(), state.fileSelected(),
                            state.uploading(), state.missingVideo(), state.uploadFailed(), page.url(), TextSupport.truncate(state.body(), 300));
                    dumpDiagnostics(page, taskId, "upload-wait-" + checks);
                }
                if (state.uploadFailed()) {
                    dumpDiagnostics(page, taskId, "upload-error");
                    throw new RuntimeException("Jinritoutiao video upload failed");
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
        throw new RuntimeException("Timed out waiting for Jinritoutiao video upload to complete, lastState=" + lastState);
    }

    private void scrollToSubmitArea(Page page, String taskId) {
        try {
            Locator formButtons = page.getByText("发布", new Page.GetByTextOptions().setExact(true)).first();
            if (formButtons.count() > 0) {
                formButtons.scrollIntoViewIfNeeded();
                page.waitForTimeout(1000);
                log.info("Jinritoutiao upload scrolled to submit area taskId={} method=publish-button", taskId);
                return;
            }
        } catch (Exception exception) {
            log.debug("Jinritoutiao upload submit area scroll by locator skipped taskId={} message={}", taskId, exception.getMessage());
        }
        try {
            page.evaluate("() => window.scrollTo({top: document.body.scrollHeight, behavior: 'instant'})");
            page.waitForTimeout(1000);
            log.info("Jinritoutiao upload scrolled to submit area taskId={} method=window-bottom", taskId);
        } catch (Exception exception) {
            log.warn("Jinritoutiao upload submit area scroll failed taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void clickSubmit(Page page, String taskId, String buttonText, String successUrlPattern) {
        RuntimeException last = null;
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(6).toMillis();
        int attempts = 0;
        while (System.currentTimeMillis() < deadline && attempts < 3) {
            attempts += 1;
            waitForUploadComplete(page, taskId);
            dismissKnownDialogs(page, taskId);
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
                    log.warn("Jinritoutiao upload submit did not reach list in time taskId={} button={} attempt={} url={}", taskId, buttonText, attempts, page.url());
                }
                page.waitForTimeout(3000);
                String body = PlaywrightDiagnostics.safeBodyText(page);
                if (!page.url().contains("/profile_v4/xigua/upload-video")
                        || TextSupport.containsAny(body, "发布成功", "保存成功", "存草稿成功", "作品管理", "审核中")) {
                    return;
                }
                if (TextSupport.containsAny(body, MISSING_VIDEO_TEXTS.toArray(String[]::new))) {
                    dumpDiagnostics(page, taskId, "submit-missing-video-" + attempts);
                    log.warn("Jinritoutiao upload submit reported missing video taskId={} attempt={} body={}",
                            taskId, attempts, TextSupport.truncate(body, 300));
                    page.waitForTimeout(5000);
                    continue;
                }
                last = new RuntimeException("Jinritoutiao submit did not finish, currentUrl=" + page.url());
            } catch (RuntimeException exception) {
                last = exception;
                log.warn("Jinritoutiao upload submit retry taskId={} button={} attempt={} message={}",
                        taskId, buttonText, attempts, exception.getMessage());
                page.waitForTimeout(3000);
            }
        }
        dumpDiagnostics(page, taskId, "submit-failed");
        throw last == null ? new RuntimeException("Jinritoutiao submit did not finish") : last;
    }

    private void waitForVideoFileAccepted(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                UploadReadiness state = uploadReadiness(page);
                if (state.fileSelected() || state.mediaVisible()) {
                    log.info("Jinritoutiao upload video file accepted taskId={} checks={} fileSelected={} mediaVisible={}",
                            taskId, checks, state.fileSelected(), state.mediaVisible());
                    return;
                }
            } catch (Exception ignored) {
            }
            page.waitForTimeout(1000);
        }
        dumpDiagnostics(page, taskId, "video-file-not-accepted");
        log.warn("Jinritoutiao upload selected video was not detected by early DOM check taskId={}, continue to metadata/upload readiness checks", taskId);
    }

    private void waitForMetadataForm(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                if (hasVisibleLocator(page, String.join(", ", TITLE_SELECTORS)) || hasMetadataFormText(page)) {
                    log.info("Jinritoutiao upload metadata form ready taskId={} checks={}", taskId, checks);
                    return;
                }
                if (checks == 1 || checks % 10 == 0) {
                    log.info("Jinritoutiao upload waiting metadata form taskId={} checks={} body={}",
                            taskId, checks, TextSupport.truncate(PlaywrightDiagnostics.safeBodyText(page), 300));
                    dumpDiagnostics(page, taskId, "metadata-form-wait-" + checks);
                }
            } catch (Exception ignored) {
            }
            page.waitForTimeout(3000);
        }
        dumpDiagnostics(page, taskId, "metadata-form-timeout");
        throw new RuntimeException("Timed out waiting for Jinritoutiao metadata form");
    }

    private boolean hasMetadataFormText(Page page) {
        String body = PlaywrightDiagnostics.safeBodyText(page);
        return TextSupport.containsAny(body, "发布设置", "标题", "封面")
                && TextSupport.containsAny(body, "上传成功", "重新上传", "删除")
                && TextSupport.containsAny(body, "发布", "存草稿");
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
                || body.contains("上传成功")
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
                log.info("Jinritoutiao upload filled {} taskId={} selector={}", label, taskId, selector);
                return;
            } catch (RuntimeException exception) {
                last = exception;
            }
        }
        dumpDiagnostics(page, taskId, "fill-" + label + "-failed");
        throw last == null ? new RuntimeException("Jinritoutiao " + label + " field not found") : last;
    }

    private void fillFirstVisibleOptional(Page page, List<String> selectors, String value, String taskId, String label) {
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
                log.info("Jinritoutiao upload filled optional {} taskId={} selector={}", label, taskId, selector);
                return;
            } catch (RuntimeException exception) {
                last = exception;
            }
        }
        log.info("Jinritoutiao upload optional {} field skipped taskId={} lastMessage={}",
                label, taskId, last == null ? "not-found" : last.getMessage());
    }

    private boolean hasVisibleLocator(Page page, String selector) {
        try {
            Locator locator = page.locator(selector).first();
            return locator.count() > 0 && locator.isVisible();
        } catch (Exception exception) {
            return false;
        }
    }

    private boolean hasVisibleLoginGate(Page page) {
        String body = PlaywrightDiagnostics.safeBodyText(page);
        return TextSupport.containsAny(body, "扫码登录", "登录/注册", "请使用今日头条扫码", "手机号登录")
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

    private UploadMaterialResolver.ResolvedFile resolveVideo(JinritoutiaoUploadRequest request) throws IOException {
        return materialResolver.resolveVideo(request.videoLocation(), request.videoUrl(), request.minioUrl(), request.videoPath(), request.alidriveFileId(), request.alidriveRemotePath(), request.taskId());
    }

    private UploadMaterialResolver.ResolvedFile resolveCover(JinritoutiaoUploadRequest request) throws IOException {
        return materialResolver.resolveCover(request.coverPath(), request.coverUrl(), request.taskId());
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
