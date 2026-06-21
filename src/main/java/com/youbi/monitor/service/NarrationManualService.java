package com.youbi.monitor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youbi.monitor.repository.MonitorRepository;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class NarrationManualService {
    private static final int MAX_SEGMENT_CHARS = 500;
    private static final long MAX_IMAGE_BYTES = 20L * 1024 * 1024;
    private final MonitorRepository repository;
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String minioBucket;

    public NarrationManualService(
            MonitorRepository repository,
            ObjectMapper objectMapper,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.minioEndpoint = trimTrailingSlash(minioEndpoint);
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
    }

    @Transactional
    public Map<String, Object> submitSegments(String taskId, String rawResponse) throws IOException {
        NarrationRow narration = narration(taskId);
        JobInput jobInput = segmentJobInput(taskId);
        List<Integer> endLineIds = parseEndLineIds(rawResponse);
        validateEndLineIds(jobInput.lines(), endLineIds);
        List<Integer> compactedIds = compactEndLineIds(jobInput.lines(), endLineIds);
        List<Integer> assignments = assignments(jobInput.lines().size(), compactedIds);

        repository.update("DELETE FROM product_narration_sentence WHERE task_id = ?", taskId);
        for (int index = 0; index < jobInput.lines().size(); index++) {
            repository.update("""
                    INSERT INTO product_narration_sentence
                      (narration_id, task_id, line_index, sentence_text, segment_index)
                    VALUES (?, ?, ?, ?, ?)
                    """, narration.id(), taskId, index + 1, jobInput.lines().get(index), assignments.get(index));
        }

        markJobSuccess(taskId, "generate_segment_plan", Map.of(
                "segment_indexes", assignments,
                "manual", true
        ));
        boolean completed;
        if ("segment_plan".equals(currentSubStage(taskId))) {
            completeSegmentStage(taskId, jobInput.lines().size(), compactedIds.size());
            completed = true;
        } else {
            completed = finalizeIfComplete(taskId);
        }
        if (!completed) {
            resumePublisher(taskId);
        }
        return Map.of(
                "taskId", taskId,
                "sentenceCount", jobInput.lines().size(),
                "segmentCount", compactedIds.size(),
                "publisherCompleted", completed
        );
    }

    @Transactional
    public Map<String, Object> uploadImage(String taskId, String rawKind, MultipartFile file) throws Exception {
        ImageKind kind = ImageKind.from(rawKind);
        NarrationRow narration = narration(taskId);
        String prompt = imagePrompt(taskId, kind, narration);
        if (text(prompt).isBlank()) {
            throw new IOException(kind.label + "提示词尚未生成");
        }
        if (file == null || file.isEmpty()) {
            throw new IOException("图片文件为空");
        }
        if (file.getSize() > MAX_IMAGE_BYTES) {
            throw new IOException("图片不能超过 20 MB");
        }

        byte[] bytes = file.getBytes();
        BufferedImage image;
        try (InputStream input = new ByteArrayInputStream(bytes)) {
            image = ImageIO.read(input);
        }
        if (image == null || image.getWidth() <= 0 || image.getHeight() <= 0) {
            throw new IOException("无法识别图片内容");
        }
        if ((long) image.getWidth() * kind.ratioHeight != (long) image.getHeight() * kind.ratioWidth) {
            throw new IOException(kind.label + "必须是精确 " + kind.ratioText
                    + "，当前为 " + image.getWidth() + "x" + image.getHeight());
        }

        String contentType = normalizedImageContentType(file.getContentType());
        String extension = extension(contentType);
        String objectKey = "publisher/narration/" + safeSegment(taskId) + "/manual/"
                + kind.apiName + "-" + System.currentTimeMillis() + extension;
        try (InputStream input = new ByteArrayInputStream(bytes)) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(minioBucket)
                    .object(objectKey)
                    .stream(input, bytes.length, -1)
                    .contentType(contentType)
                    .build());
        }
        String imageUrl = minioEndpoint + "/" + minioBucket + "/" + objectKey;
        if (kind.publishMetadata) {
            repository.update(
                    "UPDATE video_info SET " + kind.columnName + " = ? WHERE task_id = ?",
                    imageUrl,
                    taskId
            );
        } else {
            repository.update(
                    "UPDATE product_narration SET " + kind.columnName + " = ?, error_message = NULL WHERE task_id = ?",
                    imageUrl,
                    taskId
            );
        }
        markJobSuccess(taskId, kind.jobName, Map.of(
                kind.resultKey, imageUrl,
                "width", image.getWidth(),
                "height", image.getHeight(),
                "manual", true
        ));
        boolean completed = kind.publishMetadata
                ? finalizePublishMetadataIfComplete(taskId)
                : finalizeIfComplete(taskId);
        return Map.of(
                "taskId", taskId,
                "kind", kind.apiName,
                "imageUrl", imageUrl,
                "width", image.getWidth(),
                "height", image.getHeight(),
                "publisherCompleted", completed
        );
    }

    private String imagePrompt(String taskId, ImageKind kind, NarrationRow narration) throws IOException {
        List<String> rows = repository.queryForList("""
                SELECT input_json
                FROM publisher_jobs
                WHERE task_id = ? AND job_name = ?
                """, String.class, taskId, kind.jobName);
        if (!rows.isEmpty() && !text(rows.get(0)).isBlank()) {
            JsonNode input = objectMapper.readTree(rows.get(0));
            String prompt = input.path("prompt").asText("").trim();
            if (!prompt.isBlank()) {
                return prompt;
            }
        }
        if (kind.publishMetadata) {
            return "";
        }
        String basePrompt = kind == ImageKind.COVER ? narration.coverPrompt() : narration.backgroundPrompt();
        if (text(basePrompt).isBlank()) {
            return "";
        }
        return text(basePrompt).replaceFirst("[。\\s]+$", "")
                + "。严格使用 " + kind.ratioText + " 比例构图。";
    }

    private NarrationRow narration(String taskId) throws IOException {
        List<NarrationRow> rows = repository.query("""
                SELECT id, cover_prompt, cover_image_url, background_prompt, background_image_url
                FROM product_narration
                WHERE task_id = ?
                """, (rs, rowNum) -> new NarrationRow(
                rs.getLong("id"),
                rs.getString("cover_prompt"),
                rs.getString("cover_image_url"),
                rs.getString("background_prompt"),
                rs.getString("background_image_url")
        ), taskId);
        if (rows.isEmpty()) {
            throw new IOException("Narration task does not exist: " + taskId);
        }
        return rows.get(0);
    }

    private JobInput segmentJobInput(String taskId) throws IOException {
        List<String> rows = repository.queryForList("""
                SELECT input_json
                FROM publisher_jobs
                WHERE task_id = ? AND job_name = 'generate_segment_plan'
                """, String.class, taskId);
        if (rows.isEmpty() || text(rows.get(0)).isBlank()) {
            throw new IOException("分段任务没有可校验的输入，请先重试一次 publisher");
        }
        JsonNode input = objectMapper.readTree(rows.get(0));
        JsonNode linesNode = input.path("lines");
        if (!linesNode.isArray() || linesNode.isEmpty()) {
            throw new IOException("分段任务输入中缺少 lines");
        }
        List<String> lines = new ArrayList<>();
        for (JsonNode node : linesNode) {
            String line = node.asText("").trim();
            if (line.isBlank()) {
                throw new IOException("分段任务输入包含空行");
            }
            lines.add(line);
        }
        return new JobInput(lines);
    }

    private List<Integer> parseEndLineIds(String rawResponse) throws IOException {
        String value = text(rawResponse);
        if (value.isBlank()) {
            throw new IOException("大模型返回内容不能为空");
        }
        if (value.startsWith("```")) {
            value = value.replaceFirst("^```(?:json)?\\s*", "").replaceFirst("\\s*```$", "");
        }
        JsonNode root;
        try {
            root = objectMapper.readTree(value);
        } catch (JsonProcessingException firstError) {
            int start = value.indexOf('{');
            int end = value.lastIndexOf('}');
            if (start < 0 || end <= start) {
                throw new IOException("大模型返回值不是合法 JSON");
            }
            root = objectMapper.readTree(value.substring(start, end + 1));
        }
        JsonNode idsNode = root.path("end_line_ids");
        if (!idsNode.isArray() || idsNode.isEmpty()) {
            throw new IOException("返回 JSON 必须包含非空 end_line_ids 数组");
        }
        List<Integer> ids = new ArrayList<>();
        for (JsonNode node : idsNode) {
            if (!node.isIntegralNumber()) {
                throw new IOException("end_line_ids 只能包含整数");
            }
            ids.add(node.intValue());
        }
        return ids;
    }

    private void validateEndLineIds(List<String> lines, List<Integer> ids) throws IOException {
        int previous = 0;
        for (int end : ids) {
            if (end <= previous || end > lines.size()) {
                throw new IOException("end_line_ids 必须严格递增且不能超过总行数 " + lines.size());
            }
            int chars = segmentChars(lines, previous, end);
            if (chars > MAX_SEGMENT_CHARS) {
                throw new IOException("第 " + (previous + 1) + "-" + end + " 行合并后为 "
                        + chars + " 字，超过 " + MAX_SEGMENT_CHARS);
            }
            previous = end;
        }
        if (ids.get(ids.size() - 1) != lines.size()) {
            throw new IOException("end_line_ids 最后一个值必须是末行 ID " + lines.size());
        }
    }

    private List<Integer> compactEndLineIds(List<String> lines, List<Integer> ids) throws IOException {
        List<Integer> compacted = new ArrayList<>();
        int compactedStart = 0;
        int previousEnd = 0;
        for (int end : ids) {
            if (segmentChars(lines, compactedStart, end) > MAX_SEGMENT_CHARS) {
                if (previousEnd <= compactedStart) {
                    throw new IOException("无法生成不超过 " + MAX_SEGMENT_CHARS + " 字的分段");
                }
                compacted.add(previousEnd);
                compactedStart = previousEnd;
            }
            previousEnd = end;
        }
        if (compacted.isEmpty() || compacted.get(compacted.size() - 1) != lines.size()) {
            compacted.add(lines.size());
        }
        return compacted;
    }

    private int segmentChars(List<String> lines, int startInclusive, int endExclusive) {
        int chars = Math.max(0, endExclusive - startInclusive - 1);
        for (int index = startInclusive; index < endExclusive; index++) {
            chars += lines.get(index).length();
        }
        return chars;
    }

    private List<Integer> assignments(int lineCount, List<Integer> endLineIds) {
        List<Integer> result = new ArrayList<>(lineCount);
        int start = 0;
        for (int segment = 1; segment <= endLineIds.size(); segment++) {
            int end = endLineIds.get(segment - 1);
            while (start < end) {
                result.add(segment);
                start++;
            }
        }
        return result;
    }

    private void markJobSuccess(String taskId, String jobName, Map<String, Object> result) throws JsonProcessingException {
        repository.update("""
                UPDATE publisher_jobs
                SET status = 'success',
                    result_json = ?,
                    error_message = NULL,
                    completed_at = NOW()
                WHERE task_id = ? AND job_name = ?
                """, objectMapper.writeValueAsString(result), taskId, jobName);
    }

    private void resumePublisher(String taskId) {
        repository.update("""
                UPDATE publisher
                SET status = 'ready',
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id = ?
                """, taskId);
        repository.update("""
                INSERT INTO publisher_result (task_id, status, error_message)
                VALUES (?, 'running', NULL)
                ON DUPLICATE KEY UPDATE status = 'running', error_message = NULL
                """, taskId);
        repository.update("""
                UPDATE task
                SET status = 'running',
                    current_stage = 'publisher',
                    completed_at = NULL,
                    error_message = NULL
                WHERE id = ?
                """, taskId);
    }

    private String currentSubStage(String taskId) {
        List<String> rows = repository.queryForList(
                "SELECT sub_stage FROM publisher WHERE task_id = ?",
                String.class,
                taskId
        );
        return rows.isEmpty() || text(rows.get(0)).isBlank() ? "main" : text(rows.get(0));
    }

    private void completeSegmentStage(String taskId, int sentenceCount, int segmentCount) throws JsonProcessingException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("task_id", taskId);
        result.put("task_type", "narration");
        result.put("sub_stage", "segment_plan");
        result.put("sentence_count", sentenceCount);
        result.put("segment_count", segmentCount);
        result.put("manual_completed", true);
        String resultJson = objectMapper.writeValueAsString(result);

        repository.update("""
                INSERT INTO publisher_result (task_id, status, result_json, error_message)
                VALUES (?, 'success', ?, NULL)
                ON DUPLICATE KEY UPDATE
                    status = 'success',
                    result_json = VALUES(result_json),
                    error_message = NULL
                """, taskId, resultJson);
        repository.update("""
                UPDATE publisher
                SET status = 'success',
                    completed_at = NOW(),
                    error_message = NULL
                WHERE task_id = ?
                """, taskId);
        restoreRunningTask(taskId);
    }

    private boolean finalizeIfComplete(String taskId) throws JsonProcessingException {
        NarrationRow narration = narrationUnchecked(taskId);
        Integer sentenceCount = repository.queryForObject(
                "SELECT COUNT(*) FROM product_narration_sentence WHERE task_id = ?",
                Integer.class,
                taskId
        );
        if (sentenceCount == null || sentenceCount == 0
                || text(narration.coverPrompt()).isBlank()
                || text(narration.backgroundPrompt()).isBlank()
                || text(narration.coverImageUrl()).isBlank()
                || text(narration.backgroundImageUrl()).isBlank()) {
            return false;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("task_id", taskId);
        result.put("task_type", "narration");
        result.put("sub_stage", "main");
        result.put("cover_prompt", narration.coverPrompt());
        result.put("cover_image_url", narration.coverImageUrl());
        result.put("background_prompt", narration.backgroundPrompt());
        result.put("background_image_url", narration.backgroundImageUrl());
        result.put("sentence_count", sentenceCount);
        result.put("manual_completed", true);
        String resultJson = objectMapper.writeValueAsString(result);

        repository.update("""
                UPDATE product_narration
                SET status = 'success',
                    completed_at = NOW(),
                    error_message = NULL
                WHERE task_id = ?
                """, taskId);
        repository.update("UPDATE video_info SET final_cover_url = ? WHERE task_id = ?",
                narration.backgroundImageUrl(), taskId);
        repository.update("""
                INSERT INTO publisher_result (task_id, status, result_json, error_message)
                VALUES (?, 'success', ?, NULL)
                ON DUPLICATE KEY UPDATE
                    status = 'success',
                    result_json = VALUES(result_json),
                    error_message = NULL
                """, taskId, resultJson);
        repository.update("""
                UPDATE publisher
                SET status = 'success',
                    completed_at = NOW(),
                    error_message = NULL
                WHERE task_id = ?
                """, taskId);
        restoreRunningTask(taskId);
        return true;
    }

    private boolean finalizePublishMetadataIfComplete(String taskId) throws JsonProcessingException {
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT
                  vi.upload_title,
                  vi.upload_description,
                  vi.upload_tags,
                  vi.cover_text,
                  vi.vertical_cover_url,
                  vi.horizontal_cover_url,
                  pn.image_prompt
                FROM video_info vi
                JOIN product_narration pn ON pn.task_id = vi.task_id
                WHERE vi.task_id = ?
                """, taskId);
        if (rows.isEmpty()) {
            return false;
        }
        Map<String, Object> row = rows.get(0);
        if (text(row.get("upload_title")).isBlank()
                || text(row.get("upload_description")).isBlank()
                || text(row.get("upload_tags")).isBlank()
                || text(row.get("cover_text")).isBlank()
                || text(row.get("image_prompt")).isBlank()
                || text(row.get("vertical_cover_url")).isBlank()
                || text(row.get("horizontal_cover_url")).isBlank()) {
            return false;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("task_id", taskId);
        result.put("task_type", "narration");
        result.put("sub_stage", "publish_metadata");
        result.put("image_prompt", row.get("image_prompt"));
        result.put("vertical_cover_url", row.get("vertical_cover_url"));
        result.put("horizontal_cover_url", row.get("horizontal_cover_url"));
        result.put("manual_completed", true);
        String resultJson = objectMapper.writeValueAsString(result);

        repository.update("""
                UPDATE video_info
                SET final_cover_url = horizontal_cover_url
                WHERE task_id = ?
                """, taskId);
        repository.update("""
                INSERT INTO publisher_result (task_id, status, result_json, error_message)
                VALUES (?, 'success', ?, NULL)
                ON DUPLICATE KEY UPDATE
                    status = 'success',
                    result_json = VALUES(result_json),
                    error_message = NULL
                """, taskId, resultJson);
        repository.update("""
                UPDATE publisher
                SET status = 'success',
                    completed_at = NOW(),
                    error_message = NULL
                WHERE task_id = ? AND sub_stage = 'publish_metadata'
                """, taskId);
        restoreRunningTask(taskId);
        return true;
    }

    private void restoreRunningTask(String taskId) {
        repository.update("""
                UPDATE task
                SET status = 'running',
                    current_stage = 'publisher',
                    completed_at = NULL,
                    error_message = NULL
                WHERE id = ?
                """, taskId);
    }

    private NarrationRow narrationUnchecked(String taskId) {
        return repository.query("""
                SELECT id, cover_prompt, cover_image_url, background_prompt, background_image_url
                FROM product_narration
                WHERE task_id = ?
                """, (rs, rowNum) -> new NarrationRow(
                rs.getLong("id"),
                rs.getString("cover_prompt"),
                rs.getString("cover_image_url"),
                rs.getString("background_prompt"),
                rs.getString("background_image_url")
        ), taskId).get(0);
    }

    private String normalizedImageContentType(String rawContentType) {
        String contentType = text(rawContentType).toLowerCase(Locale.ROOT);
        return switch (contentType) {
            case "image/jpeg", "image/png", "image/webp", "image/gif" -> contentType;
            default -> "image/png";
        };
    }

    private String extension(String contentType) {
        return switch (contentType) {
            case "image/jpeg" -> ".jpg";
            case "image/webp" -> ".webp";
            case "image/gif" -> ".gif";
            default -> ".png";
        };
    }

    private static String safeSegment(String value) {
        String safe = text(value).replaceAll("[^A-Za-z0-9._-]+", "_");
        return safe.isBlank() ? "task" : safe;
    }

    private static String trimTrailingSlash(String value) {
        String result = text(value);
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String text(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private record JobInput(List<String> lines) {
    }

    private record NarrationRow(
            long id,
            String coverPrompt,
            String coverImageUrl,
            String backgroundPrompt,
            String backgroundImageUrl
    ) {
    }

    private enum ImageKind {
        COVER("cover", "封面图", "cover_image_url", "generate_cover_image", "cover_image_url", 1, 1, "1:1", false),
        BACKGROUND("background", "背景图", "background_image_url", "generate_background_image", "background_image_url", 4, 3, "4:3", false),
        VERTICAL("vertical", "竖版封面", "vertical_cover_url", "generate_narration_vertical_cover", "vertical_cover_url", 3, 4, "3:4", true),
        HORIZONTAL("horizontal", "横版封面", "horizontal_cover_url", "generate_narration_horizontal_cover", "horizontal_cover_url", 4, 3, "4:3", true);

        private final String apiName;
        private final String label;
        private final String columnName;
        private final String jobName;
        private final String resultKey;
        private final int ratioWidth;
        private final int ratioHeight;
        private final String ratioText;
        private final boolean publishMetadata;

        ImageKind(
                String apiName,
                String label,
                String columnName,
                String jobName,
                String resultKey,
                int ratioWidth,
                int ratioHeight,
                String ratioText,
                boolean publishMetadata
        ) {
            this.apiName = apiName;
            this.label = label;
            this.columnName = columnName;
            this.jobName = jobName;
            this.resultKey = resultKey;
            this.ratioWidth = ratioWidth;
            this.ratioHeight = ratioHeight;
            this.ratioText = ratioText;
            this.publishMetadata = publishMetadata;
        }

        private static ImageKind from(String value) throws IOException {
            for (ImageKind kind : values()) {
                if (kind.apiName.equals(text(value).toLowerCase(Locale.ROOT))) {
                    return kind;
                }
            }
            throw new IOException("图片类型必须是 cover、background、vertical 或 horizontal");
        }
    }
}
