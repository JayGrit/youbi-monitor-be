package com.youbi.monitor;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.CDPSession;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class BilibiliPlaywrightUploadService {
    private static final Logger log = LoggerFactory.getLogger(BilibiliPlaywrightUploadService.class);
    private static final String PUBLISH_VIDEO_URL = BilibiliPlaywrightAccountService.PUBLISH_VIDEO_URL;

    private final BilibiliPlaywrightAccountService accountService;
    private final Path uploadWorkDir;
    private final Path diagnosticsRoot;
    private final Path browserUploadWorkDir;
    private final UploadMaterialResolver materialResolver;
    private final SocialHumanActions humanActions;

    public BilibiliPlaywrightUploadService(
            BilibiliPlaywrightAccountService accountService,
            SocialHumanActions humanActions,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir,
            @Value("${youbi.bilibili.playwright.browser-upload-work-dir:}") String browserUploadWorkDir
    ) {
        this.accountService = accountService;
        this.humanActions = humanActions;
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        String bucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.uploadWorkDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
        this.diagnosticsRoot = this.uploadWorkDir.resolve("diagnostics").resolve("bilibili-playwright");
        this.browserUploadWorkDir = text(browserUploadWorkDir).isBlank() ? null : Path.of(browserUploadWorkDir).toAbsolutePath().normalize();
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).followRedirects(HttpClient.Redirect.NORMAL).build();
        this.materialResolver = new UploadMaterialResolver(minioClient, bucket, this.uploadWorkDir, httpClient, log, "Bilibili Playwright", false);
    }

    public BilibiliUploadResult upload(BilibiliUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        log.info("Bilibili Playwright upload start taskId={} accountKey={} videoPath={} videoUrl={} minioUrl={} title={}",
                taskId, accountKey, text(request.videoPath()), text(request.videoUrl()), text(request.minioUrl()), text(request.title()));
        UploadMaterialResolver.ResolvedFile resolvedVideo = resolveVideo(request);
        UploadMaterialResolver.ResolvedFile resolvedCover = resolveCover(request);
        try {
            Path videoPath = resolvedVideo.path();
            if (!Files.isRegularFile(videoPath) || Files.size(videoPath) == 0) {
                throw new IOException("Video file does not exist or is empty: " + videoPath);
            }
            if (resolvedCover != null && (!Files.isRegularFile(resolvedCover.path()) || Files.size(resolvedCover.path()) == 0)) {
                throw new IOException("Cover file does not exist or is empty: " + resolvedCover.path());
            }

            String storageState = accountService.storageState(accountKey);
            try (BilibiliPlaywrightAccountService.BrowserHandle browserHandle = accountService.openUploadBrowser()) {
                BrowserContext context = accountService.newContext(browserHandle.browser(), storageState);
                try {
                    Page page = context.newPage();
                    uploadVideoContent(
                            page,
                            request,
                            new UploadPaths(videoPath, browserPath(videoPath)),
                            resolvedCover == null ? null : new UploadPaths(resolvedCover.path(), browserPath(resolvedCover.path())),
                            browserHandle.browserSideFiles(),
                            taskId
                    );
                    accountService.saveStorageState(accountKey, context.storageState());
                } finally {
                    context.close();
                }
            }
            BilibiliPlaywrightAccountStatus account = accountService.status(accountKey);
            log.info("Bilibili Playwright upload done taskId={} accountKey={} elapsedMs={}", taskId, accountKey, System.currentTimeMillis() - startedAt);
            return new BilibiliUploadResult(true, "", null, account.mid(), account.uname(), "Playwright 上传流程已完成", Map.of("successUrl", PUBLISH_VIDEO_URL));
        } catch (Exception exception) {
            log.error("Bilibili Playwright upload failed taskId={} accountKey={} elapsedMs={} message={}",
                    taskId, accountKey, System.currentTimeMillis() - startedAt, exception.getMessage(), exception);
            if (exception instanceof IOException ioException) {
                throw ioException;
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Bilibili Playwright upload failed", exception);
        } finally {
            cleanup(resolvedVideo);
            cleanup(resolvedCover);
        }
    }

    public Map<String, Object> inspectUploadPage(String accountKey) throws IOException {
        String normalized = accountService.normalizeAccountKey(accountKey);
        String taskId = "inspect-" + UUID.randomUUID();
        String storageState = accountService.storageState(normalized);
        try (BilibiliPlaywrightAccountService.BrowserHandle browserHandle = accountService.openUploadBrowser()) {
            BrowserContext context = accountService.newContext(browserHandle.browser(), storageState);
            try {
                Page page = context.newPage();
                page.navigate(PUBLISH_VIDEO_URL);
                page.waitForTimeout(5000);
                PlaywrightDiagnostics.DiagnosticSnapshot snapshot = dumpDiagnostics(page, taskId, "upload-page");
                return Map.of(
                        "accountKey", normalized,
                        "url", page.url(),
                        "screenshot", snapshot.screenshot().toString(),
                        "html", snapshot.html().toString()
                );
            } finally {
                context.close();
            }
        }
    }

    public Map<String, Object> inspectUploadSelection(BilibiliUploadRequest request) throws IOException {
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        String taskId = "inspect-select-" + UUID.randomUUID();
        UploadMaterialResolver.ResolvedFile resolvedVideo = resolveVideo(request);
        try {
            String storageState = accountService.storageState(accountKey);
            try (BilibiliPlaywrightAccountService.BrowserHandle browserHandle = accountService.openUploadBrowser()) {
                BrowserContext context = accountService.newContext(browserHandle.browser(), storageState);
                try {
                    Page page = context.newPage();
                    page.navigate(PUBLISH_VIDEO_URL);
                    page.waitForTimeout(3000);
                    PlaywrightDiagnostics.DiagnosticSnapshot before = dumpDiagnostics(page, taskId, "01-before-select");
                    Locator fileInput = page.locator("input[type='file']").first();
                    fileInput.waitFor(new Locator.WaitForOptions()
                            .setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED)
                            .setTimeout(30000));
                    setInputFiles(page, "input[type='file']", new UploadPaths(resolvedVideo.path(), browserPath(resolvedVideo.path())), browserHandle.browserSideFiles(), 30000, taskId, "inspect-video");
                    page.waitForTimeout(5000);
                    PlaywrightDiagnostics.DiagnosticSnapshot selected = dumpDiagnostics(page, taskId, "02-after-select");
                    waitForMetadataForm(page, taskId);
                    PlaywrightDiagnostics.DiagnosticSnapshot form = dumpDiagnostics(page, taskId, "03-form-ready");
                    fillTitle(page, truncate(required(request.title(), "title"), 80), taskId);
                    fillDescription(page, request.description(), taskId);
                    fillTags(page, request.tags(), taskId);
                    page.waitForTimeout(1000);
                    PlaywrightDiagnostics.DiagnosticSnapshot filled = dumpDiagnostics(page, taskId, "04-metadata-filled");
                    return Map.of(
                            "accountKey", accountKey,
                            "url", page.url(),
                            "video", resolvedVideo.path().toString(),
                            "beforeScreenshot", before.screenshot().toString(),
                            "selectedScreenshot", selected.screenshot().toString(),
                            "formScreenshot", form.screenshot().toString(),
                            "filledScreenshot", filled.screenshot().toString(),
                            "filledHtml", filled.html().toString(),
                            "formHtml", form.html().toString()
                    );
                } finally {
                    context.close();
                }
            }
        } finally {
            cleanup(resolvedVideo);
        }
    }

    private void uploadVideoContent(Page page, BilibiliUploadRequest request, UploadPaths videoPaths, UploadPaths coverPaths, boolean browserSideFiles, String taskId) throws IOException {
        String title = truncate(required(request.title(), "title"), 80);
        log.info("Bilibili Playwright navigate upload page taskId={} url={}", taskId, PUBLISH_VIDEO_URL);
        page.navigate(PUBLISH_VIDEO_URL);
        page.waitForTimeout(3000);
        dumpDiagnostics(page, taskId, "01-open-upload-page");
        ensureLoggedIn(page, taskId);

        log.info("Bilibili Playwright set video input taskId={} file={} browserFile={} browserSide={}",
                taskId, videoPaths.localPath(), videoPaths.browserPath(), browserSideFiles);
        setInputFiles(page, "input[type='file']", videoPaths, browserSideFiles, 30000, taskId, "video");
        page.waitForTimeout(3000);
        dumpDiagnostics(page, taskId, "02-video-selected");

        waitForMetadataForm(page, taskId);
        fillTitle(page, title, taskId);
        fillDescription(page, request.description(), taskId);
        setCreationStatement(page, taskId);
        fillTags(page, request.tags(), taskId);
        setCoverIfPresent(page, coverPaths, browserSideFiles, taskId);
        dumpDiagnostics(page, taskId, "03-metadata-filled");
        clickPublishWhenReady(page, taskId);
    }

    private void ensureLoggedIn(Page page, String taskId) throws IOException {
        String body = safeBodyText(page);
        if (containsAny(body, "登录", "扫码登录") && !containsAny(body, "上传视频", "稿件", "发布")) {
            dumpDiagnostics(page, taskId, "login-required");
            throw new IOException("Bilibili Playwright account is not logged in. Open /api/bilibili/playwright/login/open first.");
        }
    }

    private void waitForMetadataForm(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(30).toMillis();
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            if (hasEditableTitle(page)) {
                log.info("Bilibili Playwright metadata form ready taskId={} attempts={}", taskId, attempts);
                return;
            }
            if (attempts == 1 || attempts % 30 == 0) {
                log.info("Bilibili Playwright waiting upload taskId={} attempts={} body={}", taskId, attempts, truncate(safeBodyText(page), 300));
                dumpDiagnostics(page, taskId, "upload-wait-" + attempts);
            }
            page.waitForTimeout(2000);
        }
        dumpDiagnostics(page, taskId, "metadata-timeout");
        throw new RuntimeException("Timed out waiting for Bilibili upload metadata form");
    }

    private boolean hasEditableTitle(Page page) {
        for (String selector : List.of("input[placeholder='请输入稿件标题']", "input[placeholder*='标题']", "textarea[placeholder*='标题']")) {
            try {
                Locator locator = page.locator(selector).first();
                if (locator.count() > 0 && locator.isVisible()) {
                    return true;
                }
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private void fillTitle(Page page, String title, String taskId) {
        for (String selector : List.of("input[placeholder='请输入稿件标题']", "input[placeholder*='标题']", "textarea[placeholder*='标题']")) {
            try {
                Locator locator = page.locator(selector).first();
                if (locator.count() > 0 && locator.isVisible()) {
                    humanActions.fill(page, locator, title);
                    log.info("Bilibili Playwright title filled taskId={} selector={}", taskId, selector);
                    return;
                }
            } catch (Exception ignored) {
            }
        }
        log.warn("Bilibili Playwright title selector not found taskId={}", taskId);
    }

    private void fillDescription(Page page, String description, String taskId) {
        if (!hasText(description)) {
            return;
        }
        for (String selector : List.of("div.ql-editor[data-placeholder*='相关信息']", "div.ql-editor[data-placeholder*='简介']", "textarea[placeholder*='简介']", "textarea[placeholder*='描述']", "[contenteditable='true']")) {
            try {
                Locator locator = page.locator(selector).first();
                if (locator.count() > 0 && locator.isVisible()) {
                    humanActions.fill(page, locator, text(description));
                    log.info("Bilibili Playwright description filled taskId={} selector={}", taskId, selector);
                    return;
                }
            } catch (Exception ignored) {
            }
        }
        log.warn("Bilibili Playwright description selector not found taskId={}", taskId);
    }

    private void setCreationStatement(Page page, String taskId) {
        try {
            Locator input = page.locator("input[placeholder='请选择符合您视频内容的创作声明']").first();
            if (input.count() == 0 || !input.isVisible()) {
                log.info("Bilibili Playwright creation statement not required taskId={}", taskId);
                return;
            }
            String value = text(input.inputValue(new Locator.InputValueOptions().setTimeout(1000)));
            if (!value.isBlank()) {
                log.info("Bilibili Playwright creation statement already set taskId={} value={}", taskId, value);
                return;
            }
            humanActions.click(page, input);
            page.waitForTimeout(500);
            Locator option = page.locator(".bcc-select-list-wrap:visible .bcc-option:has-text('内容无需标注'), .bcc-option:has-text('内容无需标注'):visible").last();
            option.waitFor(new Locator.WaitForOptions().setTimeout(5000));
            humanActions.click(page, option);
            log.info("Bilibili Playwright creation statement selected taskId={} value=内容无需标注", taskId);
        } catch (Exception exception) {
            log.warn("Bilibili Playwright creation statement skipped taskId={} message={}", taskId, exception.getMessage());
            dumpDiagnostics(page, taskId, "creation-statement-skipped");
        }
    }

    private void fillTags(Page page, String tags, String taskId) {
        for (String tag : parseTags(tags)) {
            try {
                Locator input = page.locator("input[placeholder='按回车键Enter创建标签'], input[placeholder*='标签'], input[placeholder*='tag']").first();
                if (input.count() == 0 || !input.isVisible()) {
                    log.warn("Bilibili Playwright tag input not found taskId={} tag={}", taskId, tag);
                    return;
                }
                humanActions.fill(page, input, tag);
                page.keyboard().press("Enter");
                page.waitForTimeout(500);
                log.info("Bilibili Playwright tag filled taskId={} tag={}", taskId, tag);
            } catch (Exception exception) {
                log.warn("Bilibili Playwright tag skipped taskId={} tag={} message={}", taskId, tag, exception.getMessage());
            }
        }
    }

    private void setCoverIfPresent(Page page, UploadPaths coverPaths, boolean browserSideFiles, String taskId) {
        if (coverPaths == null) {
            return;
        }
        try {
            String selector = "input[type='file'][accept*='image']";
            Locator coverInput = page.locator(selector).first();
            if (coverInput.count() > 0) {
                setInputFiles(page, selector, coverPaths, browserSideFiles, 30000, taskId, "cover");
                page.waitForTimeout(1000);
                log.info("Bilibili Playwright cover input filled taskId={} cover={} browserCover={}",
                        taskId, coverPaths.localPath(), coverPaths.browserPath());
                return;
            }
            log.warn("Bilibili Playwright cover input not found taskId={} cover={}", taskId, coverPaths.localPath());
        } catch (Exception exception) {
            log.warn("Bilibili Playwright cover skipped taskId={} cover={} message={}", taskId, coverPaths.localPath(), exception.getMessage());
            dumpDiagnostics(page, taskId, "cover-skipped");
        }
    }

    private void setInputFiles(Page page, String selector, UploadPaths paths, boolean browserSideFiles, double timeoutMs, String taskId, String label) throws IOException {
        Locator input = page.locator(selector).first();
        input.waitFor(new Locator.WaitForOptions()
                .setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED)
                .setTimeout(timeoutMs));
        if (!browserSideFiles) {
            input.setInputFiles(paths.localPath(), new Locator.SetInputFilesOptions().setTimeout(timeoutMs));
            return;
        }
        setInputFilesOverCdp(page, selector, paths.browserPath(), taskId, label);
    }

    private void setInputFilesOverCdp(Page page, String selector, Path browserPath, String taskId, String label) throws IOException {
        CDPSession session = page.context().newCDPSession(page);
        try {
            JsonObject documentParams = new JsonObject();
            documentParams.addProperty("pierce", true);
            JsonObject document = session.send("DOM.getDocument", documentParams);
            int rootNodeId = document.getAsJsonObject("root").get("nodeId").getAsInt();

            JsonObject queryParams = new JsonObject();
            queryParams.addProperty("nodeId", rootNodeId);
            queryParams.addProperty("selector", selector);
            JsonObject queryResult = session.send("DOM.querySelector", queryParams);
            int nodeId = queryResult.get("nodeId").getAsInt();
            if (nodeId == 0) {
                throw new IOException("Cannot find Bilibili file input over CDP: " + selector);
            }

            JsonArray files = new JsonArray();
            files.add(browserPath.toString());
            JsonObject setParams = new JsonObject();
            setParams.addProperty("nodeId", nodeId);
            setParams.add("files", files);
            session.send("DOM.setFileInputFiles", setParams);
            log.info("Bilibili Playwright set input over CDP taskId={} label={} browserFile={}", taskId, label, browserPath);
        } catch (RuntimeException exception) {
            throw new IOException("Cannot set Bilibili " + label + " file input over CDP with browser-visible path "
                    + browserPath + ": " + exception.getMessage(), exception);
        } finally {
            try {
                session.detach();
            } catch (Exception ignored) {
            }
        }
    }

    private Path browserPath(Path localPath) {
        Path normalized = localPath.toAbsolutePath().normalize();
        if (browserUploadWorkDir == null || !normalized.startsWith(uploadWorkDir)) {
            return normalized;
        }
        return browserUploadWorkDir.resolve(uploadWorkDir.relativize(normalized)).toAbsolutePath().normalize();
    }

    private void clickPublishWhenReady(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(30).toMillis();
        int attempts = 0;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            try {
                String body = safeBodyText(page);
                log.info("Bilibili Playwright publish wait taskId={} attempt={} url={} body={}", taskId, attempts, page.url(), truncate(body, 300));
                if (attempts == 1 || attempts % 30 == 0) {
                    dumpDiagnostics(page, taskId, "publish-wait-" + attempts);
                }
                if (containsAny(body, "上传中", "处理中", "转码中")) {
                    page.waitForTimeout(2000);
                    continue;
                }
                if (clickPublishButton(page, taskId)) {
                    waitForPublishAccepted(page, taskId);
                    dumpDiagnostics(page, taskId, "04-after-publish-click");
                    return;
                }
            } catch (RuntimeException exception) {
                last = exception;
                if (attempts % 10 == 1) {
                    log.warn("Bilibili Playwright publish retry taskId={} attempt={} message={}", taskId, attempts, exception.getMessage());
                }
            }
            page.waitForTimeout(2000);
        }
        dumpDiagnostics(page, taskId, "publish-timeout");
        throw last == null ? new RuntimeException("Timed out publishing Bilibili video") : last;
    }

    private boolean clickPublishButton(Page page, String taskId) {
        for (String text : List.of("立即投稿", "投稿", "发布")) {
            try {
                Locator button = page.locator("button:has-text('" + text + "'):visible, span.submit-add:has-text('" + text + "'):visible, .submit-add:has-text('" + text + "'):visible").last();
                if (button.count() > 0 && button.isEnabled()) {
                    humanActions.click(page, button);
                    log.info("Bilibili Playwright publish clicked taskId={} text={}", taskId, text);
                    return true;
                }
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private void waitForPublishAccepted(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            String body = safeBodyText(page);
            if (containsAny(body, "稿件处理进度", "稿件审核", "稿件上传", "上传完成")) {
                log.info("Bilibili Playwright publish accepted taskId={} attempts={} url={}", taskId, attempts, page.url());
                return;
            }
            if (attempts % 5 == 1) {
                log.info("Bilibili Playwright waiting publish accepted taskId={} attempts={} url={} body={}",
                        taskId, attempts, page.url(), truncate(body, 300));
            }
            page.waitForTimeout(2000);
        }
        dumpDiagnostics(page, taskId, "publish-accept-timeout");
        throw new RuntimeException("Timed out waiting for Bilibili publish accepted page");
    }

    private PlaywrightDiagnostics.DiagnosticSnapshot dumpDiagnostics(Page page, String taskId, String label) {
        return PlaywrightDiagnostics.dump(page, diagnosticsRoot, taskId, label, log, "Bilibili Playwright", true);
    }

    private String safeBodyText(Page page) {
        return PlaywrightDiagnostics.safeBodyText(page);
    }

    private UploadMaterialResolver.ResolvedFile resolveVideo(BilibiliUploadRequest request) throws IOException {
        return materialResolver.resolveVideo(request.videoUrl(), request.minioUrl(), request.videoPath(), request.taskId());
    }

    private UploadMaterialResolver.ResolvedFile resolveCover(BilibiliUploadRequest request) throws IOException {
        return materialResolver.resolveCover(request.coverPath(), request.coverUrl(), request.taskId());
    }

    private void cleanup(UploadMaterialResolver.ResolvedFile resolvedFile) {
        materialResolver.cleanupQuietly(resolvedFile);
    }

    private List<String> parseTags(String tags) {
        return Arrays.stream(text(tags).split("[,，\\s]+"))
                .map(this::text)
                .filter(value -> !value.isBlank())
                .distinct()
                .limit(10)
                .toList();
    }

    private boolean containsAny(String value, String... needles) {
        return TextSupport.containsAny(value, needles);
    }

    private boolean hasText(String value) {
        return TextSupport.hasText(value);
    }

    private String required(String value, String field) throws IOException {
        return TextSupport.required(value, field);
    }

    private String firstText(String... values) {
        return TextSupport.firstText(values);
    }

    private String truncate(String value, int max) {
        return TextSupport.truncateWithEllipsis(value, max);
    }

    private String text(String value) {
        return TextSupport.text(value);
    }

    private record UploadPaths(Path localPath, Path browserPath) {
    }
}
