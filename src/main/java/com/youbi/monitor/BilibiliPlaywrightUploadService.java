package com.youbi.monitor;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.CDPSession;
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

@Service
public class BilibiliPlaywrightUploadService {
    private static final Logger log = LoggerFactory.getLogger(BilibiliPlaywrightUploadService.class);
    private static final String PUBLISH_VIDEO_URL = BilibiliPlaywrightAccountService.PUBLISH_VIDEO_URL;

    private final BilibiliPlaywrightAccountService accountService;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final Path uploadWorkDir;
    private final Path browserUploadWorkDir;
    private final HttpClient httpClient;

    public BilibiliPlaywrightUploadService(
            BilibiliPlaywrightAccountService accountService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir,
            @Value("${youbi.bilibili.playwright.browser-upload-work-dir:}") String browserUploadWorkDir
    ) {
        this.accountService = accountService;
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.uploadWorkDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
        this.browserUploadWorkDir = text(browserUploadWorkDir).isBlank() ? null : Path.of(browserUploadWorkDir).toAbsolutePath().normalize();
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).followRedirects(HttpClient.Redirect.NORMAL).build();
    }

    public BilibiliUploadResult upload(BilibiliUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        log.info("Bilibili Playwright upload start taskId={} accountKey={} videoPath={} videoUrl={} minioUrl={} title={}",
                taskId, accountKey, text(request.videoPath()), text(request.videoUrl()), text(request.minioUrl()), text(request.title()));
        ResolvedFile resolvedVideo = resolveVideo(request);
        ResolvedFile resolvedCover = resolveCover(request);
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
                BrowserContext context = browserHandle.browser().newContext(accountService.storageContextOptions(storageState));
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
            BrowserContext context = browserHandle.browser().newContext(accountService.storageContextOptions(storageState));
            try {
                Page page = context.newPage();
                page.navigate(PUBLISH_VIDEO_URL);
                page.waitForTimeout(5000);
                DiagnosticSnapshot snapshot = dumpDiagnostics(page, taskId, "upload-page");
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
        ResolvedFile resolvedVideo = resolveVideo(request);
        try {
            String storageState = accountService.storageState(accountKey);
            try (BilibiliPlaywrightAccountService.BrowserHandle browserHandle = accountService.openUploadBrowser()) {
                BrowserContext context = browserHandle.browser().newContext(accountService.storageContextOptions(storageState));
                try {
                    Page page = context.newPage();
                    page.navigate(PUBLISH_VIDEO_URL);
                    page.waitForTimeout(3000);
                    DiagnosticSnapshot before = dumpDiagnostics(page, taskId, "01-before-select");
                    Locator fileInput = page.locator("input[type='file']").first();
                    fileInput.waitFor(new Locator.WaitForOptions()
                            .setState(com.microsoft.playwright.options.WaitForSelectorState.ATTACHED)
                            .setTimeout(30000));
                    setInputFiles(page, "input[type='file']", new UploadPaths(resolvedVideo.path(), browserPath(resolvedVideo.path())), browserHandle.browserSideFiles(), 30000, taskId, "inspect-video");
                    page.waitForTimeout(5000);
                    DiagnosticSnapshot selected = dumpDiagnostics(page, taskId, "02-after-select");
                    waitForMetadataForm(page, taskId);
                    DiagnosticSnapshot form = dumpDiagnostics(page, taskId, "03-form-ready");
                    fillTitle(page, truncate(required(request.title(), "title"), 80), taskId);
                    fillDescription(page, request.description(), taskId);
                    fillTags(page, request.tags(), taskId);
                    page.waitForTimeout(1000);
                    DiagnosticSnapshot filled = dumpDiagnostics(page, taskId, "04-metadata-filled");
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
                    locator.fill(title);
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
                    locator.fill(text(description));
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
            input.click(new Locator.ClickOptions().setTimeout(5000));
            page.waitForTimeout(500);
            Locator option = page.locator(".bcc-select-list-wrap:visible .bcc-option:has-text('内容无需标注'), .bcc-option:has-text('内容无需标注'):visible").last();
            option.waitFor(new Locator.WaitForOptions().setTimeout(5000));
            option.click(new Locator.ClickOptions().setTimeout(5000));
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
                input.fill(tag);
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
                    button.click(new Locator.ClickOptions().setTimeout(5000));
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

    private DiagnosticSnapshot dumpDiagnostics(Page page, String taskId, String label) {
        try {
            Path dir = uploadWorkDir.resolve("diagnostics").resolve("bilibili-playwright").resolve(safeSegment(taskId));
            Files.createDirectories(dir);
            Path screenshot = dir.resolve(label + ".png");
            Path html = dir.resolve(label + ".html");
            page.screenshot(new Page.ScreenshotOptions().setPath(screenshot).setFullPage(true).setTimeout(10000));
            Files.writeString(html, page.content(), StandardCharsets.UTF_8);
            log.info("Bilibili Playwright diagnostics dumped taskId={} label={} screenshot={} html={}", taskId, label, screenshot, html);
            return new DiagnosticSnapshot(screenshot, html);
        } catch (Exception exception) {
            log.warn("Bilibili Playwright diagnostics dump failed taskId={} label={} message={}", taskId, label, exception.getMessage());
            throw new RuntimeException("Cannot dump Bilibili Playwright diagnostics: " + exception.getMessage(), exception);
        }
    }

    private String safeBodyText(Page page) {
        try {
            return page.locator("body").innerText(new Locator.InnerTextOptions().setTimeout(3000));
        } catch (Exception exception) {
            return "";
        }
    }

    private ResolvedFile resolveVideo(BilibiliUploadRequest request) throws IOException {
        String minioUrl = firstText(request.videoUrl(), request.minioUrl());
        if (!minioUrl.isBlank()) {
            return new ResolvedFile(downloadMinioFile(minioUrl, request.taskId(), "video.mp4"), true);
        }
        return new ResolvedFile(Path.of(required(request.videoPath(), "videoPath")).toAbsolutePath().normalize(), false);
    }

    private ResolvedFile resolveCover(BilibiliUploadRequest request) throws IOException {
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

    private void cleanup(ResolvedFile resolvedFile) {
        if (resolvedFile == null || !resolvedFile.temporary()) {
            return;
        }
        try {
            Files.deleteIfExists(resolvedFile.path());
        } catch (Exception exception) {
            log.warn("Bilibili Playwright cleanup failed path={} message={}", resolvedFile.path(), exception.getMessage());
        }
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
        String text = text(value);
        for (String needle : needles) {
            if (text.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasText(String value) {
        return !text(value).isBlank();
    }

    private String required(String value, String field) throws IOException {
        String text = text(value);
        if (text.isBlank()) {
            throw new IOException("Missing field: " + field);
        }
        return text;
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

    private String truncate(String value, int max) {
        String text = text(value);
        if (text.codePointCount(0, text.length()) <= max) {
            return text;
        }
        return text.codePoints()
                .limit(max - 3L)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .append("...")
                .toString();
    }

    private String decode(String value) {
        return URLDecoder.decode(text(value), StandardCharsets.UTF_8);
    }

    private String sanitizeFilename(String value) {
        String sanitized = text(value).replaceAll("[\\\\/:*?\"<>|]+", "_");
        return sanitized.isBlank() ? "video.mp4" : sanitized;
    }

    private String safeSegment(String value) {
        String sanitized = text(value).replaceAll("[^A-Za-z0-9._-]+", "_");
        return sanitized.isBlank() ? "manual" : sanitized;
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record ObjectRef(String bucket, String objectName) {
    }

    private record ResolvedFile(Path path, boolean temporary) {
    }

    private record UploadPaths(Path localPath, Path browserPath) {
    }

    private record DiagnosticSnapshot(Path screenshot, Path html) {
    }
}
