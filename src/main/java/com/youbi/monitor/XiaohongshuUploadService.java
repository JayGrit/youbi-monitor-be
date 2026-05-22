package com.youbi.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class XiaohongshuUploadService {
    private static final Logger log = LoggerFactory.getLogger(XiaohongshuUploadService.class);
    private static final String PUBLISH_VIDEO_URL = XiaohongshuAccountService.PUBLISH_VIDEO_URL;
    private static final String SUCCESS_URL_PATTERN = "**/publish/success?**";

    private final XiaohongshuAccountService accountService;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final Path uploadWorkDir;
    private final HttpClient httpClient;

    public XiaohongshuUploadService(
            XiaohongshuAccountService accountService,
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

    public XiaohongshuUploadResult upload(XiaohongshuUploadRequest request) throws IOException {
        long startedAt = System.currentTimeMillis();
        String taskId = firstText(request.taskId(), "manual");
        String accountKey = accountService.normalizeAccountKey(request.accountKey());
        log.info("XHS upload start taskId={} accountKey={} videoPath={} videoUrl={} minioUrl={} title={}",
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
            log.info("XHS upload material ready taskId={} video={} videoSizeBytes={} temporaryVideo={} cover={}",
                    taskId, videoPath, Files.size(videoPath), resolvedVideo.temporary(), resolvedCover == null ? "" : resolvedCover.path());

            String storageState = accountService.storageState(accountKey);
            log.info("XHS upload storage state loaded taskId={} accountKey={} bytes={}", taskId, accountKey, storageState.getBytes(StandardCharsets.UTF_8).length);
            try (Browser browser = accountService.launchBrowser()) {
                log.info("XHS upload browser launched taskId={} accountKey={}", taskId, accountKey);
                BrowserContext context = browser.newContext(accountService.storageContextOptions(storageState));
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
            cleanup(resolvedVideo);
            cleanup(resolvedCover);
        }
    }

    private void uploadVideoContent(Page page, XiaohongshuUploadRequest request, Path videoPath, Path coverPath, String taskId) throws IOException {
        String title = truncate(required(request.title(), "title"), 20);
        log.info("XHS upload navigate publish page taskId={} url={}", taskId, PUBLISH_VIDEO_URL);
        page.navigate(PUBLISH_VIDEO_URL);
        page.waitForURL(PUBLISH_VIDEO_URL, new Page.WaitForURLOptions().setTimeout(30000));
        log.info("XHS upload set video input taskId={} file={}", taskId, videoPath);
        page.locator("div[class^='upload-content'] input[class='upload-input']").setInputFiles(videoPath);
        waitForVideoReady(page, taskId);

        Locator titleInput = page.locator("input[placeholder*='填写标题']").first();
        titleInput.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        log.info("XHS upload fill metadata taskId={} title={} tags={}", taskId, title, text(request.tags()));
        titleInput.fill(title);
        fillDescriptionAndTags(page, request.description(), request.tags(), taskId);
        setCover(page, coverPath, taskId);
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
        throw new RuntimeException("Timed out waiting for Xiaohongshu video upload to become editable");
    }

    private void fillDescriptionAndTags(Page page, String description, String tags, String taskId) {
        Locator desc = page.locator("p[data-placeholder*='输入正文描述']").first();
        desc.click();
        if (hasText(description)) {
            log.info("XHS upload fill description taskId={} chars={}", taskId, text(description).length());
            page.keyboard().press("Meta+A");
            page.keyboard().press("Backspace");
            page.keyboard().type(text(description));
            page.keyboard().press("Enter");
        }
        for (String tag : parseTags(tags)) {
            log.info("XHS upload fill tag taskId={} tag={}", taskId, tag);
            page.keyboard().type("#" + tag, new com.microsoft.playwright.Keyboard.TypeOptions().setDelay(30));
            Locator topic = page.locator("#creator-editor-topic-container").first();
            topic.waitFor(new Locator.WaitForOptions().setTimeout(5000));
            Locator first = page.locator("#creator-editor-topic-container .item").first();
            first.waitFor(new Locator.WaitForOptions().setTimeout(3000));
            first.click();
        }
    }

    private void setCover(Page page, Path coverPath, String taskId) {
        if (coverPath == null) {
            return;
        }
        log.info("XHS upload set cover taskId={} cover={}", taskId, coverPath);
        Locator coverDialog = page.locator("div.cover-plugin-title").filter(new Locator.FilterOptions().setHasText("设置封面"))
                .locator("xpath=ancestor::div[contains(@class, 'cover-plugin-preview')]")
                .locator("div.cover > div.default:visible");
        coverDialog.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        coverDialog.click(new Locator.ClickOptions().setForce(true));
        Locator modal = page.locator("div.d-modal.cover-modal").first();
        modal.waitFor(new Locator.WaitForOptions().setTimeout(30000));
        modal.locator("input[type='file'][accept*='image']").first().setInputFiles(coverPath);
        page.waitForTimeout(2000);
        modal.locator("button.mojito-button").filter(new Locator.FilterOptions().setHasText("确定")).first().click();
        modal.waitFor(new Locator.WaitForOptions().setState(com.microsoft.playwright.options.WaitForSelectorState.HIDDEN).setTimeout(30000));
    }

    private void setSchedule(Page page, String schedule, String taskId) {
        if (!hasText(schedule)) {
            return;
        }
        log.info("XHS upload set schedule taskId={} schedule={}", taskId, text(schedule));
        page.locator(".custom-switch-card").filter(new Locator.FilterOptions().setHasText("定时发布"))
                .locator(".d-switch").first().click();
        page.waitForTimeout(1000);
        page.locator(".d-datepicker-input-filter input.d-text").first().fill(text(schedule));
        page.waitForTimeout(1000);
    }

    private void clickPublish(Page page, boolean scheduled, String taskId) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        String buttonText = scheduled ? "定时发布" : "发布";
        RuntimeException last = null;
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts += 1;
            try {
                Locator buttons = page.locator("button:has-text('" + buttonText + "'):visible");
                int count = buttons.count();
                String buttonTexts = visibleButtonTexts(page);
                log.info("XHS upload click publish taskId={} button={} attempt={} url={} visibleMatches={} visibleButtons={}",
                        taskId, buttonText, attempts, page.url(), count, truncate(buttonTexts, 500));
                if (count == 0) {
                    throw new RuntimeException("No visible publish button. Visible buttons: " + buttonTexts);
                }
                buttons.last().click(new Locator.ClickOptions().setTimeout(5000));
                page.waitForURL(SUCCESS_URL_PATTERN, new Page.WaitForURLOptions().setTimeout(5000));
                log.info("XHS upload publish success page reached taskId={} url={}", taskId, page.url());
                return;
            } catch (RuntimeException exception) {
                last = exception;
                if (attempts % 10 == 1) {
                    log.warn("XHS upload publish retry taskId={} attempt={} message={}", taskId, attempts, exception.getMessage());
                }
                page.waitForTimeout(1000);
            }
        }
        throw last == null ? new RuntimeException("Timed out publishing Xiaohongshu video") : last;
    }

    private String visibleButtonTexts(Page page) {
        try {
            return String.join(" | ", page.locator("button:visible").allTextContents().stream()
                    .map(this::text)
                    .filter(value -> !value.isBlank())
                    .toList());
        } catch (Exception exception) {
            return "cannot-read-buttons: " + exception.getMessage();
        }
    }

    private ResolvedFile resolveVideo(XiaohongshuUploadRequest request) throws IOException {
        String minioUrl = firstText(request.videoUrl(), request.minioUrl());
        if (!minioUrl.isBlank()) {
            return new ResolvedFile(downloadMinioFile(minioUrl, request.taskId(), "video.mp4"), true);
        }
        return new ResolvedFile(Path.of(required(request.videoPath(), "videoPath")).toAbsolutePath().normalize(), false);
    }

    private ResolvedFile resolveCover(XiaohongshuUploadRequest request) throws IOException {
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
        log.info("XHS upload download minio start taskId={} bucket={} object={} destination={}", firstText(taskId, "manual"), objectRef.bucket(), objectRef.objectName(), destination);
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
        log.info("XHS upload download minio done taskId={} bytes={} elapsedMs={}", firstText(taskId, "manual"), Files.size(destination), System.currentTimeMillis() - startedAt);
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

    private boolean containsAny(String text, String... values) {
        String source = text(text);
        for (String value : values) {
            if (source.contains(value)) {
                return true;
            }
        }
        return false;
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
