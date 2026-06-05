package com.youbi.monitor.service;

import com.youbi.monitor.dto.AliDriveDownloadRequest;
import com.youbi.monitor.dto.AliDriveTransferResult;
import com.youbi.monitor.dto.DouyinAccountStatus;
import com.youbi.monitor.dto.DouyinUploadRequest;
import com.youbi.monitor.dto.DouyinUploadResult;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DouyinUploadService {
    private static final Logger log = LoggerFactory.getLogger(DouyinUploadService.class);
    private static final String PUBLISH_VIDEO_URL = DouyinAccountService.PUBLISH_VIDEO_URL;
    private static final String POST_VIDEO_URL_PATTERN = "**/creator-micro/content/post/video?**";
    private static final String MANAGE_URL_PATTERN = "**/creator-micro/content/manage**";
    private static final String DIAGNOSTIC_PLATFORM = "douyin";
    private static final String DIAGNOSTIC_SOURCE = "douyin-upload";
    private static final Duration PUBLISH_TIMEOUT = Duration.ofMinutes(8);

    private final DouyinAccountService accountService;
    private final AliDriveService aliDriveService;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final Path uploadWorkDir;
    private final Path profileRootDir;
    private final HttpClient httpClient;
    private final ConcurrentHashMap<String, Object> accountLocks = new ConcurrentHashMap<>();
    private final SocialHumanActions humanActions;
    private final SocialRiskDetector riskDetector;
    private final DiagnosticArtifactService diagnosticArtifactService;
    private final UploaderAttemptService uploaderAttemptService;
    private final ThreadLocal<DiagnosticRunContext> diagnosticContext = new ThreadLocal<>();

    public DouyinUploadService(
            DouyinAccountService accountService,
            SocialHumanActions humanActions,
            SocialRiskDetector riskDetector,
            DiagnosticArtifactService diagnosticArtifactService,
            UploaderAttemptService uploaderAttemptService,
            AliDriveService aliDriveService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir,
            @Value("${youbi.douyin.profile-root-dir:/work/douyin-chrome-profiles}") String profileRootDir
    ) {
        this.accountService = accountService;
        this.humanActions = humanActions;
        this.riskDetector = riskDetector;
        this.diagnosticArtifactService = diagnosticArtifactService;
        this.uploaderAttemptService = uploaderAttemptService;
        this.aliDriveService = aliDriveService;
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.uploadWorkDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
        this.profileRootDir = Path.of(text(profileRootDir).isBlank() ? "/work/douyin-chrome-profiles" : text(profileRootDir)).toAbsolutePath().normalize();
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).followRedirects(HttpClient.Redirect.NORMAL).build();
    }

    public DouyinUploadResult upload(DouyinUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        log.info("Douyin upload start taskId={} accountKey={} videoPath={} videoUrl={} minioUrl={} title={}",
                taskId, accountKey, text(request.videoPath()), text(request.videoUrl()), text(request.minioUrl()), text(request.title()));
        ResolvedFile resolvedVideo = resolveVideo(request);
        ResolvedFile resolvedCover = resolveCover(request);
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
            log.info("Douyin upload material ready taskId={} video={} videoSizeBytes={} temporaryVideo={} cover={}",
                    taskId, videoPath, Files.size(videoPath), resolvedVideo.temporary(), resolvedCover == null ? "" : resolvedCover.path());

            Object lock = accountLocks.computeIfAbsent(accountKey, ignored -> new Object());
            synchronized (lock) {
                Path profileDir = profileDir(accountKey);
                Files.createDirectories(profileDir);
                log.info("Douyin upload launch persistent Chrome taskId={} accountKey={} profileDir={}", taskId, accountKey, profileDir);
                try (BrowserContext context = accountService.launchPersistentContext(profileDir)) {
                    Page page = context.newPage();
                    try {
                        uploadVideoContent(
                                page,
                                request,
                                new UploadPaths(videoPath, videoPath),
                                resolvedCover == null ? null : new UploadPaths(resolvedCover.path(), resolvedCover.path()),
                                false,
                                taskId
                        );
                        accountService.saveStorageState(accountKey, context.storageState());
                        log.info("Douyin upload persistent storage state saved taskId={} accountKey={}", taskId, accountKey);
                    } finally {
                        page.close();
                    }
                }
            }
            DouyinAccountStatus account = accountService.status(accountKey);
            log.info("Douyin upload success taskId={} accountKey={} elapsedMs={}", taskId, accountKey, System.currentTimeMillis() - startedAt);
            return new DouyinUploadResult(true, accountKey, account.nickname(), "上传成功", Map.of("successUrl", PUBLISH_VIDEO_URL));
        } catch (Exception exception) {
            log.error("Douyin upload failed taskId={} accountKey={} elapsedMs={} message={}", taskId, accountKey, System.currentTimeMillis() - startedAt, exception.getMessage(), exception);
            if (exception instanceof IOException ioException) {
                throw ioException;
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Douyin upload failed", exception);
        } finally {
            diagnosticContext.remove();
            cleanup(resolvedVideo);
            cleanup(resolvedCover);
        }
    }

    private void uploadVideoContent(Page page, DouyinUploadRequest request, UploadPaths videoPaths, UploadPaths coverPaths, boolean browserSideFiles, String taskId) throws IOException {
        String title = truncate(required(request.title(), "title"), 30);
        log.info("Douyin upload navigate publish page taskId={} url={}", taskId, PUBLISH_VIDEO_URL);
        page.navigate(PUBLISH_VIDEO_URL);
        page.waitForURL(PUBLISH_VIDEO_URL, new Page.WaitForURLOptions().setTimeout(30000));
        ensureUploadLandingPage(page, taskId, request.accountKey());
        log.info("Douyin upload set video input taskId={} file={} browserFile={} browserSide={}",
                taskId, videoPaths.localPath(), videoPaths.browserPath(), browserSideFiles);
        setInputFiles(page, "div[class^='container'] input[type='file']", videoPaths, browserSideFiles, 300000, taskId, "video");
        waitForPublishPage(page, taskId);
        page.waitForTimeout(1000);

        log.info("Douyin upload fill metadata taskId={} title={} tags={}", taskId, title, text(request.tags()));
        fillTitleDescriptionAndTags(page, title, firstText(request.description(), title), request.tags());
        waitForVideoUploaded(page, videoPaths, browserSideFiles, taskId);
        setProductLink(page, request.productLink(), request.productTitle(), taskId);
        setCover(page, coverPaths, browserSideFiles, taskId);
        enableThirdPartySyncIfPresent(page, taskId);
        setSchedule(page, request.schedule(), taskId);
        clickPublish(page, taskId);
    }

    private void ensureUploadLandingPage(Page page, String taskId, String accountKey) throws IOException {
        try {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            while (System.currentTimeMillis() < deadline) {
                SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.DOUYIN, page);
                if (risk.blocking() && !"login_required".equals(risk.code())) {
                    dumpDiagnostics(page, taskId, "upload-page-risk-blocked");
                    throw new IOException("Douyin upload blocked: " + risk.message());
                }
                if (page.locator("div[class^='container'] input[type='file']").count() > 0) {
                    return;
                }
                if (hasVisibleLoginGate(page)) {
                    dumpDiagnostics(page, taskId, "upload-page-login-required");
                    throw new IOException("Douyin login state is expired or rejected by the publish page. Please refresh accountKey="
                            + accountService.normalizeAccountKey(accountKey) + " before uploading. url=" + page.url());
                }
                page.waitForTimeout(500);
            }
            dumpDiagnostics(page, taskId, "upload-page-not-ready");
            throw new IOException("Douyin upload page is not ready. url=" + page.url());
        } catch (IOException exception) {
            throw exception;
        } catch (Exception exception) {
            dumpDiagnostics(page, taskId, "upload-page-check-failed");
            throw new IOException("Cannot verify Douyin upload page: " + exception.getMessage(), exception);
        }
    }

    private boolean hasVisibleLoginGate(Page page) {
        for (Locator marker : List.of(
                page.getByText("扫码登录").first(),
                page.getByText("手机号登录").first(),
                page.getByText("验证码登录").first(),
                page.getByText("创作者登录").first(),
                page.getByText("登录/注册").first(),
                page.getByRole(com.microsoft.playwright.options.AriaRole.IMG, new Page.GetByRoleOptions().setName("二维码")).first(),
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

    private void waitForPublishPage(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(15).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                page.waitForURL("**/creator-micro/content/publish?enter_from=publish_page", new Page.WaitForURLOptions().setTimeout(3000));
                log.info("Douyin upload entered publish page v1 taskId={}", taskId);
                return;
            } catch (Exception ignored) {
            }
            try {
                page.waitForURL(POST_VIDEO_URL_PATTERN, new Page.WaitForURLOptions().setTimeout(3000));
                log.info("Douyin upload entered publish page v2 taskId={}", taskId);
                return;
            } catch (Exception ignored) {
            }
            log.debug("Douyin upload waiting publish page taskId={} url={}", taskId, page.url());
            page.waitForTimeout(500);
        }
        throw new RuntimeException("Timed out waiting for Douyin publish page");
    }

    private void fillTitleDescriptionAndTags(Page page, String title, String description, String tags) {
        Locator descriptionSection = page.getByText("作品描述", new Page.GetByTextOptions().setExact(true))
                .locator("xpath=ancestor::div[2]")
                .locator("xpath=following-sibling::div[1]");
        Locator titleInput = descriptionSection.locator("input[type='text']").first();
        titleInput.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        humanActions.fill(page, titleInput, title);

        Locator descriptionEditor = descriptionSection.locator(".zone-container[contenteditable='true']").first();
        descriptionEditor.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        humanActions.click(page, descriptionEditor);
        page.keyboard().press("Control+A");
        page.keyboard().press("Delete");
        humanActions.type(page, text(description));
        for (String tag : parseTags(tags)) {
            humanActions.type(page, " #" + tag);
            page.keyboard().press("Space");
        }
    }

    private void waitForVideoUploaded(Page page, UploadPaths videoPaths, boolean browserSideFiles, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(30).toMillis();
        int checks = 0;
        while (System.currentTimeMillis() < deadline) {
            checks += 1;
            try {
                if (page.locator("[class^='long-card'] div:has-text('重新上传')").count() > 0) {
                    log.info("Douyin upload video uploaded taskId={} checks={}", taskId, checks);
                    return;
                }
                if (page.locator("div.progress-div > div:has-text('上传失败')").count() > 0) {
                    log.warn("Douyin upload retry failed upload taskId={} file={} browserFile={}",
                            taskId, videoPaths.localPath(), videoPaths.browserPath());
                    setInputFiles(page, "div.progress-div [class^='upload-btn-input']", videoPaths, browserSideFiles, 300000, taskId, "video-retry");
                }
            } catch (Exception ignored) {
            }
            if (checks % 10 == 1) {
                log.info("Douyin upload waiting video taskId={} checks={}", taskId, checks);
            }
            page.waitForTimeout(2000);
        }
        dumpDiagnostics(page, taskId, "video-upload-timeout");
        SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.DOUYIN, page);
        if (risk.blocking()) {
            throw new RuntimeException("Douyin upload blocked while waiting video upload: " + risk.message());
        }
        throw new RuntimeException("Timed out waiting for Douyin video upload");
    }

    private void setCover(Page page, UploadPaths coverPaths, boolean browserSideFiles, String taskId) throws IOException {
        if (coverPaths == null) {
            return;
        }
        log.info("Douyin upload set cover taskId={} cover={} browserCover={} browserSide={}",
                taskId, coverPaths.localPath(), coverPaths.browserPath(), browserSideFiles);
        humanActions.click(page, page.locator("text=\"选择封面\"").first());
        Locator modal = page.locator("div[id*='creator-content-modal']").first();
        modal.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        setInputFiles(page, "div[id*='creator-content-modal'] div[class^='semi-upload upload'] input.semi-upload-hidden-input", coverPaths, browserSideFiles, 120000, taskId, "cover");
        page.waitForTimeout(2000);
        humanActions.click(page, modal.locator("button:visible:has-text('完成')").first());
        page.waitForSelector("div.extractFooter", new Page.WaitForSelectorOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.DETACHED).setTimeout(30000));
    }

    private void setInputFiles(Page page, String selector, UploadPaths paths, boolean browserSideFiles, double timeoutMs, String taskId, String label) throws IOException {
        Locator input = page.locator(selector).first();
        input.waitFor(new Locator.WaitForOptions()
                .setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED)
                .setTimeout(timeoutMs));
        if (browserSideFiles) {
            throw new IOException("Douyin CDP browser-side file upload has been removed");
        }
        input.setInputFiles(paths.localPath(), new Locator.SetInputFilesOptions().setTimeout(timeoutMs));
    }

    private Path profileDir(String accountKey) {
        return profileRootDir.resolve(safeSegment(accountKey)).toAbsolutePath().normalize();
    }

    private void setProductLink(Page page, String productLink, String productTitle, String taskId) {
        if (!hasText(productLink) || !hasText(productTitle)) {
            return;
        }
        log.info("Douyin upload set product link taskId={} productTitle={}", taskId, truncate(productTitle, 10));
        try {
            page.waitForSelector("text=添加标签", new Page.WaitForSelectorOptions().setTimeout(10000));
            Locator dropdown = page.getByText("添加标签")
                    .locator("..")
                    .locator("..")
                    .locator("..")
                    .locator(".semi-select")
                    .first();
            if (dropdown.count() == 0) {
                log.warn("Douyin upload product dropdown not found taskId={}", taskId);
                return;
            }
            humanActions.click(page, dropdown);
            page.waitForSelector("[role='listbox']", new Page.WaitForSelectorOptions().setTimeout(5000));
            humanActions.click(page, page.locator("[role='option']:has-text('购物车')").first());

            Locator input = page.locator("input[placeholder='粘贴商品链接']").first();
            input.waitFor(new Locator.WaitForOptions().setTimeout(5000));
            humanActions.fill(page, input, text(productLink));
            Locator addButton = page.locator("span:has-text('添加链接')").first();
            String buttonClass = addButton.getAttribute("class");
            if (buttonClass != null && buttonClass.contains("disable")) {
                log.warn("Douyin upload product add button disabled taskId={}", taskId);
                return;
            }
            humanActions.click(page, addButton);
            page.waitForTimeout(2000);

            Locator invalidModal = page.locator("text=未搜索到对应商品").first();
            if (invalidModal.count() > 0 && invalidModal.isVisible()) {
                humanActions.click(page, page.locator("button:has-text('确定')").first());
                log.warn("Douyin upload product link invalid taskId={}", taskId);
                return;
            }
            handleProductDialog(page, productTitle, taskId);
        } catch (Exception exception) {
            log.warn("Douyin upload product link skipped taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void handleProductDialog(Page page, String productTitle, String taskId) {
        try {
            Locator shortTitleInput = page.locator("input[placeholder='请输入商品短标题']").first();
            shortTitleInput.waitFor(new Locator.WaitForOptions().setTimeout(10000));
            humanActions.fill(page, shortTitleInput, truncate(productTitle, 10));
            page.waitForTimeout(1000);
            Locator finishButton = page.locator("button:has-text('完成编辑')").first();
            String buttonClass = finishButton.getAttribute("class");
            if (buttonClass == null || !buttonClass.contains("disabled")) {
                humanActions.click(page, finishButton);
                page.waitForSelector(".semi-modal-content", new Page.WaitForSelectorOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.HIDDEN).setTimeout(5000));
                log.info("Douyin upload product dialog completed taskId={}", taskId);
                return;
            }
            Locator cancelButton = page.locator("button:has-text('取消')").first();
            if (cancelButton.count() > 0) {
                humanActions.click(page, cancelButton);
            } else {
                humanActions.click(page, page.locator(".semi-modal-close").first());
            }
            page.waitForSelector(".semi-modal-content", new Page.WaitForSelectorOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.HIDDEN).setTimeout(5000));
        } catch (Exception exception) {
            log.warn("Douyin upload product dialog skipped taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void enableThirdPartySyncIfPresent(Page page, String taskId) {
        String selector = "[class^='info'] > [class^='first-part'] div div.semi-switch";
        try {
            Locator element = page.locator(selector).first();
            if (element.count() == 0) {
                return;
            }
            String className = String.valueOf(element.evaluate("node => node.className"));
            if (!className.contains("semi-switch-checked")) {
                humanActions.click(page, element.locator("input.semi-switch-native-control"));
                log.info("Douyin upload enabled third-party sync taskId={}", taskId);
            }
        } catch (Exception exception) {
            log.warn("Douyin upload third-party sync skipped taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void setSchedule(Page page, String schedule, String taskId) {
        if (!hasText(schedule)) {
            return;
        }
        log.info("Douyin upload set schedule taskId={} schedule={}", taskId, text(schedule));
        humanActions.click(page, page.locator("[class^='radio']:has-text('定时发布')").first());
        page.waitForTimeout(1000);
        Locator scheduleInput = page.locator(".semi-input[placeholder='日期和时间']").first();
        humanActions.click(page, scheduleInput);
        page.keyboard().press("Control+A");
        humanActions.type(page, text(schedule));
        page.keyboard().press("Enter");
        page.waitForTimeout(1000);
    }

    private void clickPublish(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + PUBLISH_TIMEOUT.toMillis();
        RuntimeException last = null;
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            try {
                dismissBlockingDialog(page, taskId);
                Locator publishButton = page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON,
                        new Page.GetByRoleOptions().setName("发布").setExact(true));
                if (publishButton.count() > 0) {
                    humanActions.click(page, publishButton.first());
                }
                page.waitForURL(MANAGE_URL_PATTERN, new Page.WaitForURLOptions().setTimeout(5000));
                log.info("Douyin upload publish success page reached taskId={} url={}", taskId, page.url());
                return;
            } catch (RuntimeException exception) {
                last = exception;
                handleAutoVideoCover(page, taskId);
                dismissBlockingDialog(page, taskId);
                if (attempts % 10 == 1) {
                    dumpDiagnostics(page, taskId, "publish-wait-" + attempts);
                    log.info("Douyin upload publish retry taskId={} attempt={} message={}", taskId, attempts, exception.getMessage());
                }
                page.waitForTimeout(500);
            }
        }
        dumpDiagnostics(page, taskId, "publish-timeout");
        SocialRiskState risk = riskDetector.detect(SocialBrowserPlatform.DOUYIN, page);
        if (risk.blocking()) {
            throw new RuntimeException("Douyin publish blocked: " + risk.message());
        }
        throw new RuntimeException("Timed out publishing Douyin video after " + PUBLISH_TIMEOUT.toMinutes() + " minutes", last);
    }

    private boolean dismissBlockingDialog(Page page, String taskId) {
        if (clickKnownFloatingPrompt(page, taskId)) {
            return true;
        }
        for (String text : List.of("暂不设置", "我知道了", "知道了", "稍后再说", "完成", "取消")) {
            if (clickDialogButtonByText(page, taskId, text)) {
                return true;
            }
        }
        try {
            Locator modal = visibleDialog(page).last();
            if (modal.count() == 0) {
                return false;
            }
            Locator primary = modal.locator("button:visible, [role='button']:visible").last();
            if (primary.count() == 0) {
                return false;
            }
            String text = primary.innerText(new Locator.InnerTextOptions().setTimeout(1000));
            clickWithFallback(page, primary);
            log.info("Douyin upload dismissed modal taskId={} button={}", taskId, truncate(text, 50));
            page.waitForTimeout(500);
            return true;
        } catch (Exception exception) {
            log.debug("Douyin upload modal dismiss skipped taskId={} message={}", taskId, exception.getMessage());
            return false;
        }
    }

    private boolean clickKnownFloatingPrompt(Page page, String taskId) {
        for (String promptText : List.of("视频预览功能")) {
            try {
                Locator prompt = page.getByText(promptText).first();
                if (prompt.count() == 0 || !prompt.isVisible()) {
                    continue;
                }
                Locator container = prompt.locator("xpath=ancestor::div[contains(@class, 'semi-popover') or contains(@class, 'tooltip') or contains(@class, 'popover')][1]");
                if (container.count() == 0) {
                    container = prompt.locator("xpath=ancestor::div[1]");
                }
                Locator button = container.getByText("我知道了", new Locator.GetByTextOptions().setExact(true)).first();
                if (button.count() == 0) {
                    button = page.getByText("我知道了", new Page.GetByTextOptions().setExact(true)).first();
                }
                if (button.count() > 0 && button.isVisible()) {
                    clickWithFallback(page, button);
                    log.info("Douyin upload dismissed floating prompt taskId={} prompt={}", taskId, promptText);
                    page.waitForTimeout(500);
                    return true;
                }
            } catch (Exception exception) {
                log.debug("Douyin upload floating prompt skipped taskId={} prompt={} message={}", taskId, promptText, exception.getMessage());
            }
        }
        return false;
    }

    private Locator visibleDialog(Page page) {
        return page.locator(String.join(", ",
                ".semi-modal-content:visible",
                ".dy-creator-content-modal-wrap:visible",
                "[role='dialog']:visible"
        ));
    }

    private boolean clickDialogButtonByText(Page page, String taskId, String buttonText) {
        try {
            Locator modal = visibleDialog(page).last();
            if (modal.count() == 0) {
                return false;
            }
            Locator button = modal.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON,
                    new Locator.GetByRoleOptions().setName(buttonText).setExact(true)).last();
            if (button.count() == 0) {
                button = modal.locator("button:visible:has-text('" + buttonText + "'), [role='button']:visible:has-text('" + buttonText + "')").last();
            }
            if (button.count() == 0) {
                return false;
            }
            clickWithFallback(page, button);
            log.info("Douyin upload dismissed modal taskId={} button={}", taskId, buttonText);
            page.waitForTimeout(800);
            return true;
        } catch (Exception exception) {
            log.debug("Douyin upload dialog button skipped taskId={} button={} message={}", taskId, buttonText, exception.getMessage());
            return false;
        }
    }

    private void clickWithFallback(Page page, Locator locator) {
        try {
            humanActions.click(page, locator);
        } catch (Exception exception) {
            locator.evaluate("element => element.click()");
        }
    }

    private void handleAutoVideoCover(Page page, String taskId) {
        try {
            Locator needCover = page.getByText("请设置封面后再发布").first();
            if (needCover.count() == 0 || !needCover.isVisible()) {
                return;
            }
            Locator recommendCover = page.locator("[class^='recommendCover-']").first();
            if (recommendCover.count() == 0) {
                return;
            }
            humanActions.click(page, recommendCover);
            page.waitForTimeout(1000);
            Locator confirm = page.getByText("是否确认应用此封面？").first();
            if (confirm.count() > 0 && confirm.isVisible()) {
                humanActions.click(page, page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON, new Page.GetByRoleOptions().setName("确定")));
            }
            log.info("Douyin upload auto cover selected taskId={}", taskId);
        } catch (Exception exception) {
            log.warn("Douyin upload auto cover skipped taskId={} message={}", taskId, exception.getMessage());
        }
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

    private ResolvedFile resolveVideo(DouyinUploadRequest request) throws IOException {
        if (isAliDriveVideo(request)) {
            return new ResolvedFile(downloadAliDriveFile(request), true);
        }
        String minioUrl = firstText(request.videoUrl(), request.minioUrl());
        if (!minioUrl.isBlank()) {
            return new ResolvedFile(downloadMinioFile(minioUrl, request.taskId(), "video.mp4"), true);
        }
        return new ResolvedFile(Path.of(required(request.videoPath(), "videoPath")).toAbsolutePath().normalize(), false);
    }

    private Path downloadAliDriveFile(DouyinUploadRequest request) throws IOException {
        AliDriveRef ref = aliDriveRef(request.videoUrl(), request.minioUrl(), request.alidriveFileId(), request.alidriveRemotePath());
        if (ref.fileId().isBlank() && ref.remotePath().isBlank()) {
            throw new IOException("AliDrive video reference is missing");
        }
        Path taskDir = uploadWorkDir.resolve(safeSegment(firstText(request.taskId(), "manual"))).resolve(UUID.randomUUID().toString());
        try {
            AliDriveTransferResult result = aliDriveService.download(new AliDriveDownloadRequest(ref.remotePath(), ref.fileId(), taskDir.toString(), ""));
            Path path = Path.of(required(result.localPath(), "localPath")).toAbsolutePath().normalize();
            if (!Files.isRegularFile(path) || Files.size(path) == 0) {
                throw new IOException("Downloaded AliDrive video is empty: " + path);
            }
            return path;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted downloading AliDrive video", exception);
        }
    }

    private ResolvedFile resolveCover(DouyinUploadRequest request) throws IOException {
        if (hasText(request.coverPath())) {
            return new ResolvedFile(Path.of(request.coverPath()).toAbsolutePath().normalize(), false);
        }
        if (!hasText(request.coverUrl())) {
            return null;
        }
        String coverUrl = text(request.coverUrl());
        if (isMinioRef(coverUrl)) {
            return new ResolvedFile(downloadMinioFile(coverUrl, request.taskId(), "cover.jpg"), true);
        }
        Path taskDir = uploadWorkDir.resolve(safeSegment(firstText(request.taskId(), "manual"))).resolve(UUID.randomUUID().toString());
        Files.createDirectories(taskDir);
        Path destination = taskDir.resolve("cover.jpg");
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(coverUrl))
                .timeout(Duration.ofMinutes(2))
                .header("User-Agent", "Mozilla/5.0")
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() / 100 != 2 || response.body().length == 0) {
                throw new IOException("Cannot download coverUrl: " + response.statusCode() + " " + coverUrl);
            }
            Files.write(destination, response.body());
            return new ResolvedFile(destination, true);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted downloading coverUrl", exception);
        }
    }

    private Path downloadMinioFile(String minioUrl, String taskId, String fallbackFilename) throws IOException {
        ObjectRef objectRef = parseObjectRef(minioUrl);
        String filename = sanitizeFilename(Path.of(objectRef.objectName()).getFileName().toString());
        if (filename.isBlank()) {
            filename = fallbackFilename;
        }
        Path taskDir = uploadWorkDir.resolve(safeSegment(firstText(taskId, "manual"))).resolve(UUID.randomUUID().toString());
        Path destination = taskDir.resolve(filename);
        Files.createDirectories(taskDir);
        long startedAt = System.currentTimeMillis();
        log.info("Douyin upload download minio start taskId={} bucket={} object={} destination={}", firstText(taskId, "manual"), objectRef.bucket(), objectRef.objectName(), destination);
        try (InputStream input = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(objectRef.bucket())
                        .object(objectRef.objectName())
                        .build()
        )) {
            Files.copy(input, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception exc) {
            throw new IOException("Cannot download MinIO file: " + minioUrl, exc);
        }
        log.info("Douyin upload download minio done taskId={} bytes={} elapsedMs={}", firstText(taskId, "manual"), Files.size(destination), System.currentTimeMillis() - startedAt);
        return destination;
    }

    private ObjectRef parseObjectRef(String ref) throws IOException {
        String value = text(ref);
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException exc) {
            throw new IOException("Invalid MinIO URL: " + ref, exc);
        }
        if ("s3".equals(uri.getScheme())) {
            return requiredObjectRef(text(uri.getHost()).isBlank() ? minioBucket : uri.getHost(), decode(uri.getPath()).replaceFirst("^/+", ""), ref);
        }
        if ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) {
            return requiredObjectRef(minioBucket, stripKnownPrefix(decode(uri.getPath())), ref);
        }
        if (value.startsWith("/minio/") || value.startsWith("/" + minioBucket + "/") || value.startsWith(minioBucket + "/")) {
            return requiredObjectRef(minioBucket, stripKnownPrefix(value), ref);
        }
        throw new IOException("Unsupported MinIO URL: " + ref);
    }

    private ObjectRef requiredObjectRef(String bucket, String objectName, String ref) throws IOException {
        String cleanObjectName = text(objectName).replaceFirst("^/+", "");
        if (cleanObjectName.isBlank()) {
            throw new IOException("Cannot resolve MinIO object from URL: " + ref);
        }
        return new ObjectRef(text(bucket).isBlank() ? minioBucket : text(bucket), cleanObjectName);
    }

    private String stripKnownPrefix(String path) {
        String value = text(path).split("\\?", 2)[0].replaceFirst("^/+", "");
        for (String prefix : List.of("minio/" + minioBucket + "/", minioBucket + "/")) {
            if (value.startsWith(prefix)) {
                return value.substring(prefix.length());
            }
        }
        return value;
    }

    private boolean isMinioRef(String ref) {
        String value = text(ref);
        if (value.startsWith("s3:") || value.startsWith("/minio/") || value.startsWith("/" + minioBucket + "/") || value.startsWith(minioBucket + "/")) {
            return true;
        }
        try {
            URI uri = URI.create(value);
            String path = decode(uri.getPath());
            return ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                    && (path.startsWith("/minio/" + minioBucket + "/") || path.startsWith("/" + minioBucket + "/"));
        } catch (Exception ignored) {
            return false;
        }
    }

    private List<String> parseTags(String tags) {
        return Arrays.stream(text(tags).split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .toList();
    }

    private void cleanup(ResolvedFile file) throws IOException {
        if (file != null && file.temporary()) {
            Files.deleteIfExists(file.path());
        }
    }

    private String truncate(String value, int max) {
        String text = text(value);
        if (text.codePointCount(0, text.length()) <= max) {
            return text;
        }
        return text.codePoints()
                .limit(max)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private String required(String value, String field) throws IOException {
        String text = text(value);
        if (text.isBlank()) {
            throw new IOException("Missing field: " + field);
        }
        return text;
    }

    private boolean hasText(String value) {
        return !text(value).isBlank();
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

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private String decode(String value) {
        return URLDecoder.decode(text(value), StandardCharsets.UTF_8);
    }

    private String sanitizeFilename(String value) {
        String sanitized = text(value).replaceAll("[\\\\/:*?\"<>|]+", "_");
        return sanitized.isBlank() ? "" : sanitized;
    }

    private String safeSegment(String value) {
        String sanitized = text(value).replaceAll("[^A-Za-z0-9._-]+", "_");
        return sanitized.isBlank() ? "manual" : sanitized;
    }

    private boolean isAliDriveVideo(DouyinUploadRequest request) {
        String location = text(request.videoLocation()).toLowerCase();
        return location.equals("adrive")
                || location.equals("alidrive")
                || location.equals("aliyun")
                || location.equals("aliyundrive")
                || hasText(request.alidriveFileId())
                || hasText(request.alidriveRemotePath())
                || isAliDriveRef(request.videoUrl())
                || isAliDriveRef(request.minioUrl());
    }

    private boolean isAliDriveRef(String ref) {
        return text(ref).startsWith("adrive://");
    }

    private AliDriveRef aliDriveRef(String videoUrl, String minioUrl, String fileId, String remotePath) {
        String resolvedFileId = text(fileId);
        String resolvedRemotePath = text(remotePath);
        String ref = firstText(videoUrl, minioUrl);
        if (resolvedFileId.isBlank() && resolvedRemotePath.isBlank() && isAliDriveRef(ref)) {
            String value = text(ref).replaceFirst("^adrive://", "").trim();
            if (value.startsWith("/")) {
                resolvedRemotePath = value;
            } else {
                resolvedFileId = value;
            }
        }
        return new AliDriveRef(resolvedFileId, resolvedRemotePath);
    }

    private record ObjectRef(String bucket, String objectName) {
    }

    private record ResolvedFile(Path path, boolean temporary) {
    }

    private record AliDriveRef(String fileId, String remotePath) {
    }

    private record UploadPaths(Path localPath, Path browserPath) {
    }
}
