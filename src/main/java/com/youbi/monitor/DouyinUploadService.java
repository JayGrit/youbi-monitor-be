package com.youbi.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
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

@Service
public class DouyinUploadService {
    private static final Logger log = LoggerFactory.getLogger(DouyinUploadService.class);
    private static final String PUBLISH_VIDEO_URL = DouyinAccountService.PUBLISH_VIDEO_URL;
    private static final String POST_VIDEO_URL_PATTERN = "**/creator-micro/content/post/video?**";
    private static final String MANAGE_URL_PATTERN = "**/creator-micro/content/manage**";

    private final DouyinAccountService accountService;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final Path uploadWorkDir;
    private final HttpClient httpClient;

    public DouyinUploadService(
            DouyinAccountService accountService,
            ObjectMapper objectMapper,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir
    ) {
        this.accountService = accountService;
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.uploadWorkDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
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

            String storageState = accountService.storageState(accountKey);
            log.info("Douyin upload storage state loaded taskId={} accountKey={} bytes={}", taskId, accountKey, storageState.getBytes(StandardCharsets.UTF_8).length);
            try (Browser browser = accountService.launchBrowser()) {
                log.info("Douyin upload browser launched taskId={} accountKey={}", taskId, accountKey);
                BrowserContext context = browser.newContext(accountService.storageContextOptions(storageState));
                try {
                    Page page = context.newPage();
                    uploadVideoContent(page, request, videoPath, resolvedCover == null ? null : resolvedCover.path(), taskId);
                    accountService.saveStorageState(accountKey, context.storageState());
                    log.info("Douyin upload storage state saved taskId={} accountKey={}", taskId, accountKey);
                } finally {
                    context.close();
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
            cleanup(resolvedVideo);
            cleanup(resolvedCover);
        }
    }

    private void uploadVideoContent(Page page, DouyinUploadRequest request, Path videoPath, Path coverPath, String taskId) throws IOException {
        String title = truncate(required(request.title(), "title"), 30);
        log.info("Douyin upload navigate publish page taskId={} url={}", taskId, PUBLISH_VIDEO_URL);
        page.navigate(PUBLISH_VIDEO_URL);
        page.waitForURL(PUBLISH_VIDEO_URL, new Page.WaitForURLOptions().setTimeout(30000));
        log.info("Douyin upload set video input taskId={} file={}", taskId, videoPath);
        page.locator("div[class^='container'] input").setInputFiles(videoPath);
        waitForPublishPage(page, taskId);
        page.waitForTimeout(1000);

        log.info("Douyin upload fill metadata taskId={} title={} tags={}", taskId, title, text(request.tags()));
        fillTitleDescriptionAndTags(page, title, firstText(request.description(), title), request.tags());
        waitForVideoUploaded(page, videoPath, taskId);
        setProductLink(page, request.productLink(), request.productTitle(), taskId);
        setCover(page, coverPath, taskId);
        enableThirdPartySyncIfPresent(page, taskId);
        setSchedule(page, request.schedule(), taskId);
        clickPublish(page, taskId);
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
        titleInput.fill(title);

        Locator descriptionEditor = descriptionSection.locator(".zone-container[contenteditable='true']").first();
        descriptionEditor.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        descriptionEditor.click();
        page.keyboard().press("Control+A");
        page.keyboard().press("Delete");
        page.keyboard().type(text(description));
        for (String tag : parseTags(tags)) {
            page.keyboard().type(" #" + tag);
            page.keyboard().press("Space");
        }
    }

    private void waitForVideoUploaded(Page page, Path videoPath, String taskId) {
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
                    log.warn("Douyin upload retry failed upload taskId={} file={}", taskId, videoPath);
                    page.locator("div.progress-div [class^='upload-btn-input']").setInputFiles(videoPath);
                }
            } catch (Exception ignored) {
            }
            if (checks % 10 == 1) {
                log.info("Douyin upload waiting video taskId={} checks={}", taskId, checks);
            }
            page.waitForTimeout(2000);
        }
        dumpDiagnostics(page, taskId, "video-upload-timeout");
        throw new RuntimeException("Timed out waiting for Douyin video upload");
    }

    private void setCover(Page page, Path coverPath, String taskId) {
        if (coverPath == null) {
            return;
        }
        log.info("Douyin upload set cover taskId={} cover={}", taskId, coverPath);
        page.click("text=\"选择封面\"");
        Locator modal = page.locator("div[id*='creator-content-modal']").first();
        modal.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        modal.locator("div[class^='semi-upload upload'] input.semi-upload-hidden-input").first().setInputFiles(coverPath);
        page.waitForTimeout(2000);
        modal.locator("button:visible:has-text('完成')").first().click();
        page.waitForSelector("div.extractFooter", new Page.WaitForSelectorOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.DETACHED).setTimeout(30000));
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
            dropdown.click();
            page.waitForSelector("[role='listbox']", new Page.WaitForSelectorOptions().setTimeout(5000));
            page.locator("[role='option']:has-text('购物车')").first().click();

            Locator input = page.locator("input[placeholder='粘贴商品链接']").first();
            input.waitFor(new Locator.WaitForOptions().setTimeout(5000));
            input.fill(text(productLink));
            Locator addButton = page.locator("span:has-text('添加链接')").first();
            String buttonClass = addButton.getAttribute("class");
            if (buttonClass != null && buttonClass.contains("disable")) {
                log.warn("Douyin upload product add button disabled taskId={}", taskId);
                return;
            }
            addButton.click();
            page.waitForTimeout(2000);

            Locator invalidModal = page.locator("text=未搜索到对应商品").first();
            if (invalidModal.count() > 0 && invalidModal.isVisible()) {
                page.locator("button:has-text('确定')").first().click();
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
            shortTitleInput.fill(truncate(productTitle, 10));
            page.waitForTimeout(1000);
            Locator finishButton = page.locator("button:has-text('完成编辑')").first();
            String buttonClass = finishButton.getAttribute("class");
            if (buttonClass == null || !buttonClass.contains("disabled")) {
                finishButton.click();
                page.waitForSelector(".semi-modal-content", new Page.WaitForSelectorOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.HIDDEN).setTimeout(5000));
                log.info("Douyin upload product dialog completed taskId={}", taskId);
                return;
            }
            Locator cancelButton = page.locator("button:has-text('取消')").first();
            if (cancelButton.count() > 0) {
                cancelButton.click();
            } else {
                page.locator(".semi-modal-close").first().click();
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
                element.locator("input.semi-switch-native-control").click();
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
        page.locator("[class^='radio']:has-text('定时发布')").first().click();
        page.waitForTimeout(1000);
        page.locator(".semi-input[placeholder='日期和时间']").first().click();
        page.keyboard().press("Control+A");
        page.keyboard().type(text(schedule));
        page.keyboard().press("Enter");
        page.waitForTimeout(1000);
    }

    private void clickPublish(Page page, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(30).toMillis();
        RuntimeException last = null;
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            try {
                dismissBlockingDialog(page, taskId);
                Locator publishButton = page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON,
                        new Page.GetByRoleOptions().setName("发布").setExact(true));
                if (publishButton.count() > 0) {
                    publishButton.first().click(new Locator.ClickOptions().setTimeout(5000));
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
        throw last == null ? new RuntimeException("Timed out publishing Douyin video") : last;
    }

    private void dismissBlockingDialog(Page page, String taskId) {
        try {
            Locator modal = page.locator(".semi-modal-content:visible").last();
            if (modal.count() == 0) {
                return;
            }
            Locator primary = modal.locator("button:visible").last();
            if (primary.count() == 0) {
                return;
            }
            String text = primary.innerText(new Locator.InnerTextOptions().setTimeout(1000));
            primary.click(new Locator.ClickOptions().setTimeout(3000));
            log.info("Douyin upload dismissed modal taskId={} button={}", taskId, truncate(text, 50));
            page.waitForTimeout(500);
        } catch (Exception exception) {
            log.debug("Douyin upload modal dismiss skipped taskId={} message={}", taskId, exception.getMessage());
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
            recommendCover.click();
            page.waitForTimeout(1000);
            Locator confirm = page.getByText("是否确认应用此封面？").first();
            if (confirm.count() > 0 && confirm.isVisible()) {
                page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON, new Page.GetByRoleOptions().setName("确定")).click();
            }
            log.info("Douyin upload auto cover selected taskId={}", taskId);
        } catch (Exception exception) {
            log.warn("Douyin upload auto cover skipped taskId={} message={}", taskId, exception.getMessage());
        }
    }

    private void dumpDiagnostics(Page page, String taskId, String label) {
        try {
            Path dir = uploadWorkDir.resolve("diagnostics").resolve(safeSegment(taskId));
            Files.createDirectories(dir);
            Path screenshot = dir.resolve("douyin-" + label + ".png");
            Path html = dir.resolve("douyin-" + label + ".html");
            page.screenshot(new Page.ScreenshotOptions().setPath(screenshot).setFullPage(true).setTimeout(10000));
            Files.writeString(html, page.content(), StandardCharsets.UTF_8);
            log.info("Douyin upload diagnostics dumped taskId={} label={} screenshot={} html={}", taskId, label, screenshot, html);
        } catch (Exception exception) {
            log.warn("Douyin upload diagnostics dump failed taskId={} label={} message={}", taskId, label, exception.getMessage());
        }
    }

    private ResolvedFile resolveVideo(DouyinUploadRequest request) throws IOException {
        String minioUrl = firstText(request.videoUrl(), request.minioUrl());
        if (!minioUrl.isBlank()) {
            return new ResolvedFile(downloadMinioFile(minioUrl, request.taskId(), "video.mp4"), true);
        }
        return new ResolvedFile(Path.of(required(request.videoPath(), "videoPath")).toAbsolutePath().normalize(), false);
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

    private record ObjectRef(String bucket, String objectName) {
    }

    private record ResolvedFile(Path path, boolean temporary) {
    }
}
