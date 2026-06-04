package com.youbi.monitor.service;

import com.microsoft.playwright.Page;
import com.youbi.monitor.model.DiagnosticArtifactRecord;
import com.youbi.monitor.repository.IDiagnosticArtifactRepositoryService;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class DiagnosticArtifactService {
    private static final Logger log = LoggerFactory.getLogger(DiagnosticArtifactService.class);
    private static final String TABLE = "uploader_diagonostic";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;

    private final IDiagnosticArtifactRepositoryService repositoryService;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final String minioEndpoint;

    public DiagnosticArtifactService(
            IDiagnosticArtifactRepositoryService repositoryService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.repositoryService = repositoryService;
        this.minioEndpoint = trimTrailingSlash(TextSupport.text(minioEndpoint));
        this.minioBucket = TextSupport.text(minioBucket).isBlank() ? "ydbi" : TextSupport.text(minioBucket);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
    }

    @PostConstruct
    void ensureSchema() {
        repositoryService.ensureSchema();
    }

    DiagnosticArtifactRecord archive(DiagnosticArtifactRequest request) {
        String taskId = TextSupport.firstText(request.taskId(), "manual");
        String runId = TextSupport.firstText(request.runId(), taskId);
        String platform = TextSupport.firstText(request.platform(), "unknown");
        String source = TextSupport.firstText(request.source(), "unknown");
        String accountKey = TextSupport.text(request.accountKey());
        String stepName = TextSupport.firstText(request.stepName(), "snapshot");
        int stepIndex = Math.max(1, request.stepIndex());
        try {
            Page page = request.page();
            byte[] screenshot = page.screenshot(new Page.ScreenshotOptions()
                    .setFullPage(true)
                    .setTimeout(10000));
            byte[] html = page.content().getBytes(StandardCharsets.UTF_8);
            ImageSize imageSize = imageSize(screenshot);
            String baseObjectKey = objectKeyPrefix(platform, source, taskId, runId, stepIndex, stepName);
            String screenshotObjectKey = baseObjectKey + ".png";
            String htmlObjectKey = baseObjectKey + ".html";
            putObject(screenshotObjectKey, screenshot, "image/png");
            putObject(htmlObjectKey, html, "text/html; charset=utf-8");
            String screenshotUrl = minioUrl(screenshotObjectKey);
            String htmlUrl = minioUrl(htmlObjectKey);
            Long id = insert(taskId, runId, platform, source, accountKey, stepIndex, stepName,
                    screenshotUrl, htmlUrl, (long) screenshot.length, (long) html.length, imageSize.width(), imageSize.height());
            log.info("Diagnostic artifact archived taskId={} runId={} platform={} source={} stepIndex={} stepName={} screenshotUrl={} htmlUrl={}",
                    taskId, runId, platform, source, stepIndex, stepName, screenshotUrl, htmlUrl);
            return new DiagnosticArtifactRecord(id, taskId, runId, platform, source, emptyToNull(accountKey), stepIndex, stepName,
                    screenshotUrl, htmlUrl, (long) screenshot.length, (long) html.length, imageSize.width(), imageSize.height(),
                    "uploaded", null, null);
        } catch (Exception exception) {
            log.warn("Diagnostic artifact archive failed taskId={} runId={} platform={} source={} stepIndex={} stepName={} message={}",
                    taskId, runId, platform, source, stepIndex, stepName, exception.getMessage());
            return new DiagnosticArtifactRecord(null, taskId, runId, platform, source, emptyToNull(accountKey), stepIndex, stepName,
                    null, null, null, null, null, null, "upload_failed", exception.getMessage(), null);
        }
    }

    public List<DiagnosticArtifactRecord> list(String taskId, String runId) {
        String normalizedTaskId = TextSupport.text(taskId);
        String normalizedRunId = TextSupport.text(runId);
        if (normalizedTaskId.isBlank()) {
            throw new IllegalArgumentException("Missing taskId");
        }
        if (normalizedRunId.isBlank()) {
            return repositoryService.listByTaskId(normalizedTaskId);
        }
        return repositoryService.listByTaskIdAndRunId(normalizedTaskId, normalizedRunId);
    }

    private Long insert(String taskId, String runId, String platform, String source, String accountKey, int stepIndex, String stepName,
                        String screenshotUrl, String htmlUrl, Long screenshotSizeBytes, Long htmlSizeBytes,
                        Integer screenshotWidth, Integer screenshotHeight) {
        return repositoryService.insertUploadedArtifact(
                taskId,
                runId,
                platform,
                source,
                emptyToNull(accountKey),
                stepIndex,
                stepName,
                screenshotUrl,
                htmlUrl,
                screenshotSizeBytes,
                htmlSizeBytes,
                screenshotWidth,
                screenshotHeight
        );
    }

    private void putObject(String objectKey, byte[] bytes, String contentType) throws Exception {
        try (ByteArrayInputStream input = new ByteArrayInputStream(bytes)) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(minioBucket)
                    .object(objectKey)
                    .stream(input, bytes.length, -1)
                    .contentType(contentType)
                    .build());
        }
    }

    private String objectKeyPrefix(String platform, String source, String taskId, String runId, int stepIndex, String stepName) {
        return String.join("/",
                "diagnostics",
                TextSupport.safeSegment(platform),
                TextSupport.safeSegment(source),
                LocalDate.now().format(DATE_FORMAT),
                TextSupport.safeSegment(taskId),
                TextSupport.safeSegment(runId),
                stepPrefix(stepIndex) + "-" + TextSupport.safeSegment(stepName));
    }

    private String stepPrefix(int stepIndex) {
        return stepIndex < 100 ? "%02d".formatted(stepIndex) : String.valueOf(stepIndex);
    }

    private String minioUrl(String objectKey) {
        return minioEndpoint + "/" + minioBucket + "/" + objectKey;
    }

    private ImageSize imageSize(byte[] screenshot) {
        try {
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(screenshot));
            if (image == null) {
                return new ImageSize(null, null);
            }
            return new ImageSize(image.getWidth(), image.getHeight());
        } catch (Exception exception) {
            return new ImageSize(null, null);
        }
    }

    private String emptyToNull(String value) {
        String text = TextSupport.text(value);
        return text.isBlank() ? null : text;
    }

    private String trimTrailingSlash(String value) {
        String text = TextSupport.text(value);
        while (text.endsWith("/")) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }

    private record ImageSize(Integer width, Integer height) {
    }
}
