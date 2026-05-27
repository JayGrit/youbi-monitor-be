package com.youbi.monitor;

import com.microsoft.playwright.Page;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class DiagnosticArtifactService {
    private static final Logger log = LoggerFactory.getLogger(DiagnosticArtifactService.class);
    private static final String TABLE = "uploader_diagonostic";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;

    private final JdbcTemplate jdbcTemplate;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final String minioEndpoint;

    public DiagnosticArtifactService(
            JdbcTemplate jdbcTemplate,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.minioEndpoint = trimTrailingSlash(TextSupport.text(minioEndpoint));
        this.minioBucket = TextSupport.text(minioBucket).isBlank() ? "ydbi" : TextSupport.text(minioBucket);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
    }

    @PostConstruct
    void ensureSchema() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS uploader_diagonostic (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    task_id VARCHAR(128) NOT NULL,
                    run_id VARCHAR(128) NOT NULL,
                    platform VARCHAR(32) NOT NULL,
                    source VARCHAR(64) NOT NULL,
                    account_key VARCHAR(128) NULL,
                    step_index INT NOT NULL,
                    step_name VARCHAR(128) NOT NULL,
                    screenshot_url TEXT NOT NULL,
                    html_url TEXT NULL,
                    screenshot_size_bytes BIGINT NULL,
                    html_size_bytes BIGINT NULL,
                    screenshot_width INT NULL,
                    screenshot_height INT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'uploaded',
                    error_message TEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    KEY idx_uploader_diag_task_order (task_id, run_id, step_index, id),
                    KEY idx_uploader_diag_platform_time (platform, created_at),
                    KEY idx_uploader_diag_source_time (source, created_at)
                )
                """);
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
            return jdbcTemplate.query("""
                    SELECT id, task_id, run_id, platform, source, account_key, step_index, step_name,
                           screenshot_url, html_url, screenshot_size_bytes, html_size_bytes,
                           screenshot_width, screenshot_height, status, error_message, created_at
                    FROM uploader_diagonostic
                    WHERE task_id = ?
                    ORDER BY created_at DESC, run_id DESC, step_index ASC, id ASC
                    """, (rs, rowNum) -> mapRecord(rs), normalizedTaskId);
        }
        return jdbcTemplate.query("""
                SELECT id, task_id, run_id, platform, source, account_key, step_index, step_name,
                       screenshot_url, html_url, screenshot_size_bytes, html_size_bytes,
                       screenshot_width, screenshot_height, status, error_message, created_at
                FROM uploader_diagonostic
                WHERE task_id = ? AND run_id = ?
                ORDER BY step_index ASC, id ASC
                """, (rs, rowNum) -> mapRecord(rs), normalizedTaskId, normalizedRunId);
    }

    private Long insert(String taskId, String runId, String platform, String source, String accountKey, int stepIndex, String stepName,
                        String screenshotUrl, String htmlUrl, Long screenshotSizeBytes, Long htmlSizeBytes,
                        Integer screenshotWidth, Integer screenshotHeight) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO uploader_diagonostic
                    (task_id, run_id, platform, source, account_key, step_index, step_name,
                     screenshot_url, html_url, screenshot_size_bytes, html_size_bytes,
                     screenshot_width, screenshot_height, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploaded')
                    """, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, taskId);
            ps.setString(2, runId);
            ps.setString(3, platform);
            ps.setString(4, source);
            ps.setString(5, emptyToNull(accountKey));
            ps.setInt(6, stepIndex);
            ps.setString(7, stepName);
            ps.setString(8, screenshotUrl);
            ps.setString(9, htmlUrl);
            ps.setObject(10, screenshotSizeBytes);
            ps.setObject(11, htmlSizeBytes);
            ps.setObject(12, screenshotWidth);
            ps.setObject(13, screenshotHeight);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        return key == null ? null : key.longValue();
    }

    private DiagnosticArtifactRecord mapRecord(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new DiagnosticArtifactRecord(
                rs.getLong("id"),
                rs.getString("task_id"),
                rs.getString("run_id"),
                rs.getString("platform"),
                rs.getString("source"),
                rs.getString("account_key"),
                rs.getInt("step_index"),
                rs.getString("step_name"),
                rs.getString("screenshot_url"),
                rs.getString("html_url"),
                nullableLong(rs, "screenshot_size_bytes"),
                nullableLong(rs, "html_size_bytes"),
                nullableInt(rs, "screenshot_width"),
                nullableInt(rs, "screenshot_height"),
                rs.getString("status"),
                rs.getString("error_message"),
                rs.getObject("created_at", LocalDateTime.class)
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

    private Long nullableLong(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
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
