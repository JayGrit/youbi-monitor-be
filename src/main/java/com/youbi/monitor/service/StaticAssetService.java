package com.youbi.monitor.service;

import com.youbi.monitor.dto.StaticAssetCreateRequest;
import com.youbi.monitor.repository.SqlRepository;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class StaticAssetService {
    private static final Set<String> TYPES = Set.of("video", "audio", "image", "voice", "text", "font");
    private static final long MAX_UPLOAD_BYTES = 200L * 1024 * 1024;

    private final SqlRepository repository;
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String minioBucket;

    public StaticAssetService(
            @Qualifier("sqlRepository") SqlRepository repository,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.repository = repository;
        this.minioEndpoint = trimTrailingSlash(text(minioEndpoint));
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
    }

    public Map<String, Object> listAssets(String type, String taskId, String scope, String keyword, Integer limit, Integer offset) {
        Query query = buildQuery(type, taskId, scope, keyword);
        int pageSize = Math.max(1, Math.min(limit == null ? 40 : limit, 100));
        int pageOffset = Math.max(0, offset == null ? 0 : offset);
        List<Object> pageArgs = new ArrayList<>(query.args());
        pageArgs.add(pageSize);
        pageArgs.add(pageOffset);

        List<Map<String, Object>> items = repository.query(
                """
                SELECT id, task_id, type, content, remark, created_at, updated_at
                FROM asseter_static
                """
                        + query.where()
                        + " ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?",
                (rs, rowNum) -> row(rs, false),
                pageArgs.toArray()
        );
        Long total = repository.queryForObject(
                "SELECT COUNT(*) FROM asseter_static " + query.where(),
                Long.class,
                query.args().toArray()
        );
        List<Map<String, Object>> typeCounts = repository.query(
                "SELECT type, COUNT(*) AS count FROM asseter_static GROUP BY type ORDER BY type",
                (rs, rowNum) -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("type", rs.getString("type"));
                    row.put("count", rs.getLong("count"));
                    return row;
                }
        );
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("items", items);
        result.put("total", total == null ? 0 : total);
        result.put("limit", pageSize);
        result.put("offset", pageOffset);
        result.put("typeCounts", typeCounts);
        return result;
    }

    public Map<String, Object> getAsset(long id) throws IOException {
        List<Map<String, Object>> rows = repository.query(
                """
                SELECT id, task_id, type, content, remark, created_at, updated_at
                FROM asseter_static
                WHERE id = ?
                """,
                (rs, rowNum) -> row(rs, true),
                id
        );
        if (rows.isEmpty()) {
            throw new IOException("素材不存在: " + id);
        }
        return rows.get(0);
    }

    public Map<String, Object> createTextAsset(StaticAssetCreateRequest request) throws IOException {
        String type = normalizeType(request == null ? null : request.type());
        if (!"text".equals(type)) {
            throw new IOException("文本创建仅支持 text 类型");
        }
        String content = text(request.content());
        if (content.isBlank()) {
            throw new IOException("内容不能为空");
        }
        return insertAsset(text(request.taskId()), type, content, text(request.remark()));
    }

    public Map<String, Object> uploadAsset(String rawType, String taskId, String remark, MultipartFile file) throws Exception {
        String type = normalizeType(rawType);
        if ("text".equals(type)) {
            throw new IOException("text 类型请直接填写内容");
        }
        if (file == null || file.isEmpty()) {
            throw new IOException("上传文件为空");
        }
        if (file.getSize() > MAX_UPLOAD_BYTES) {
            throw new IOException("文件不能超过 200 MB");
        }

        String original = text(file.getOriginalFilename());
        String extension = extension(original);
        String objectKey = "assets/" + type + "/" + LocalDateTime.now().toString().replaceAll("[^0-9]", "")
                + "-" + safeSegment(original.isBlank() ? "asset" : original.replaceAll("\\.[^.]*$", "")) + extension;
        String contentType = text(file.getContentType());
        if (contentType.isBlank()) {
            contentType = "application/octet-stream";
        }
        try (InputStream input = file.getInputStream()) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(minioBucket)
                    .object(objectKey)
                    .stream(input, file.getSize(), -1)
                    .contentType(contentType)
                    .build());
        }
        return insertAsset(text(taskId), type, minioEndpoint + "/" + minioBucket + "/" + objectKey, text(remark));
    }

    private Map<String, Object> insertAsset(String taskId, String type, String content, String remark) throws IOException {
        String normalizedTaskId = taskId.isBlank() ? null : taskId;
        if (normalizedTaskId != null && !taskExists(normalizedTaskId)) {
            throw new IOException("任务不存在: " + normalizedTaskId);
        }
        Long id = repository.insertAndReturnKey(
                """
                INSERT INTO asseter_static (task_id, type, content, remark)
                VALUES (?, ?, ?, ?)
                """,
                normalizedTaskId,
                type,
                content,
                remark.isBlank() ? null : remark
        );
        return getAsset(id == null ? 0 : id);
    }

    private boolean taskExists(String taskId) {
        Long count = repository.queryForObject("SELECT COUNT(*) FROM task WHERE id = ?", Long.class, taskId);
        return count != null && count > 0;
    }

    private Query buildQuery(String rawType, String taskId, String scope, String keyword) {
        List<String> clauses = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        String type = text(rawType);
        if (!type.isBlank()) {
            clauses.add("type = ?");
            args.add(normalizeType(type));
        }
        String normalizedScope = text(scope).toLowerCase(Locale.ROOT);
        String normalizedTaskId = text(taskId);
        if ("global".equals(normalizedScope)) {
            clauses.add("task_id IS NULL");
        } else if ("task".equals(normalizedScope) || !normalizedTaskId.isBlank()) {
            clauses.add("task_id = ?");
            args.add(normalizedTaskId);
        }
        String q = text(keyword);
        if (!q.isBlank()) {
            clauses.add("(remark LIKE ? OR content LIKE ? OR task_id LIKE ?)");
            String pattern = "%" + q + "%";
            args.add(pattern);
            args.add(pattern);
            args.add(pattern);
        }
        return new Query(clauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", clauses), args);
    }

    private static Map<String, Object> row(ResultSet rs, boolean fullContent) throws SQLException {
        String content = rs.getString("content");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getLong("id"));
        row.put("taskId", rs.getString("task_id"));
        row.put("type", rs.getString("type"));
        row.put("content", fullContent ? content : summarize(content));
        row.put("contentLength", content == null ? 0 : content.length());
        row.put("remark", rs.getString("remark"));
        row.put("createdAt", rs.getTimestamp("created_at") == null ? null : rs.getTimestamp("created_at").toLocalDateTime());
        row.put("updatedAt", rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime());
        return row;
    }

    private static String summarize(String value) {
        String normalized = text(value).replaceAll("\\s+", " ").trim();
        return normalized.length() <= 180 ? normalized : normalized.substring(0, 180) + "...";
    }

    private static String normalizeType(String value) {
        String type = text(value).toLowerCase(Locale.ROOT);
        if (!TYPES.contains(type)) {
            throw new IllegalArgumentException("不支持的素材类型: " + value);
        }
        return type;
    }

    private static String extension(String filename) {
        int dot = filename.lastIndexOf('.');
        if (dot < 0 || dot == filename.length() - 1) return "";
        String ext = filename.substring(dot).toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9.]", "");
        return ext.length() > 16 ? "" : ext;
    }

    private static String safeSegment(String value) {
        String normalized = text(value).toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]+", "-");
        normalized = normalized.replaceAll("^-+|-+$", "");
        return normalized.isBlank() ? "asset" : normalized;
    }

    private static String trimTrailingSlash(String value) {
        return value.replaceFirst("/+$", "");
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record Query(String where, List<Object> args) {
    }
}
