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
        throw last == null ? new RuntimeException("Timed out publishing Xiaohongshu video") : last;
    }

    private void clickPublishButton(Page page, String buttonText, String taskId) {
        Locator nativeButton = page.locator("button:has-text('" + buttonText + "'):visible");
        if (nativeButton.count() > 0) {
            nativeButton.last().click(new Locator.ClickOptions().setTimeout(5000));
            log.info("XHS upload publish click taskId={} method=native-button", taskId);
            return;
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
        textButton.click(new Locator.ClickOptions().setTimeout(5000));
        log.info("XHS upload publish click taskId={} method=playwright-text", taskId);
    }

    private void dumpDiagnostics(Page page, String taskId, String label) {
        try {
            Path dir = uploadWorkDir.resolve("diagnostics").resolve(safeSegment(taskId));
            Files.createDirectories(dir);
            Path screenshot = dir.resolve(label + ".png");
            Path html = dir.resolve(label + ".html");
            page.screenshot(new Page.ScreenshotOptions().setPath(screenshot).setFullPage(true).setTimeout(10000));
            Files.writeString(html, page.content(), StandardCharsets.UTF_8);
            log.info("XHS upload diagnostics dumped taskId={} label={} screenshot={} html={}", taskId, label, screenshot, html);
        } catch (Exception exception) {
            log.warn("XHS upload diagnostics dump failed taskId={} label={} message={}", taskId, label, exception.getMessage());
        }
    }

    private PageState pageState(Page page, String buttonText) {
        String body = "";
        try {
            body = page.locator("body").innerText(new Locator.InnerTextOptions().setTimeout(3000));
        } catch (Exception ignored) {
        }
        return new PageState(
                containsAny(body, "视频上传中", "上传中", "智能推荐封面生成中"),
                visibleButtonTexts(page),
                exactTextElements(page, buttonText),
                exactTextMatchCount(page, buttonText)
        );
    }

    private int exactTextMatchCount(Page page, String buttonText) {
        try {
            return page.locator("text=\"" + buttonText + "\"").count();
        } catch (Exception ignored) {
            return 0;
        }
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

    private String exactTextElements(Page page, String buttonText) {
        try {
            return page.locator("text=\"" + buttonText + "\"").evaluateAll(
                    """
                    (els) => els.map((el, i) => {
                      const rect = el.getBoundingClientRect();
                      const style = window.getComputedStyle(el);
                      return `${i}:${el.tagName}.${el.className || ''}:visible=${!!(rect.width && rect.height) && style.visibility !== 'hidden' && style.display !== 'none'}:rect=${Math.round(rect.x)},${Math.round(rect.y)},${Math.round(rect.width)},${Math.round(rect.height)}:text=${(el.innerText || el.textContent || '').trim()}`;
                    }).join(' | ')
                    """
            ).toString();
        } catch (Exception exception) {
            return "cannot-read-exact-text: " + exception.getMessage();
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

    private record PageState(boolean uploading, String visibleButtons, String exactTextElements, int exactTextMatches) {
    }
}
