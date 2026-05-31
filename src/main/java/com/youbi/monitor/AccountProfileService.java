package com.youbi.monitor;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Service
public class AccountProfileService {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;
    private static final Map<String, String> TABLES = Map.of(
            "bilibili", "uploader_account_bilibili",
            "douyin", "uploader_account_douyin",
            "xiaohongshu", "uploader_account_xiaohongshu",
            "shipinhao", "uploader_account_shipinhao",
            "kuaishou", "uploader_account_kuaishou"
    );

    private final JdbcTemplate jdbcTemplate;
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String minioBucket;

    public AccountProfileService(
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

    public AccountProfileUpdateResult updateProfile(String platform, String accountKey, AccountProfileUpdateRequest request) throws IOException {
        String table = table(platform);
        String normalized = normalizeAccountKey(accountKey);
        ensureColumns(table);
        String displayName = TextSupport.text(request == null ? "" : request.displayName());
        int updated = jdbcTemplate.update(
                "UPDATE " + table + " SET display_name = ?, updated_at = NOW() WHERE account_key = ?",
                displayName.isBlank() ? null : TextSupport.truncate(displayName, 128),
                normalized
        );
        if (updated != 1) {
            throw new IOException("Account key not found: " + normalized);
        }
        return profile(table, normalized);
    }

    public AccountProfileUpdateResult uploadAvatar(String platform, String accountKey, MultipartFile file) throws Exception {
        String table = table(platform);
        String normalized = normalizeAccountKey(accountKey);
        ensureColumns(table);
        if (file == null || file.isEmpty()) {
            throw new IOException("Avatar file is empty");
        }
        String contentType = TextSupport.text(file.getContentType());
        if (!contentType.startsWith("image/")) {
            throw new IOException("Avatar must be an image");
        }
        String objectKey = String.join("/",
                "account-avatars",
                TextSupport.safeSegment(platform),
                TextSupport.safeSegment(normalized),
                LocalDate.now().format(DATE_FORMAT),
                System.currentTimeMillis() + "-" + TextSupport.safeSegment(file.getOriginalFilename()));
        try (InputStream input = file.getInputStream()) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(minioBucket)
                    .object(objectKey)
                    .stream(input, file.getSize(), -1)
                    .contentType(contentType)
                    .build());
        }
        String avatarUrl = minioUrl(objectKey);
        int updated = jdbcTemplate.update(
                "UPDATE " + table + " SET avatar_url = ?, updated_at = NOW() WHERE account_key = ?",
                avatarUrl,
                normalized
        );
        if (updated != 1) {
            throw new IOException("Account key not found: " + normalized);
        }
        return profile(table, normalized);
    }

    private AccountProfileUpdateResult profile(String table, String accountKey) {
        return jdbcTemplate.queryForObject(
                "SELECT display_name, avatar_url FROM " + table + " WHERE account_key = ?",
                (rs, rowNum) -> new AccountProfileUpdateResult(rs.getString("display_name"), rs.getString("avatar_url")),
                accountKey
        );
    }

    private void ensureColumns(String table) {
        ensureColumn(table, "display_name", "VARCHAR(128) NULL");
        ensureColumn(table, "avatar_url", "VARCHAR(1024) NULL");
    }

    private void ensureColumn(String table, String column, String definition) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        if (count == null || count == 0) {
            jdbcTemplate.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
    }

    private String table(String platform) throws IOException {
        String table = TABLES.get(TextSupport.text(platform));
        if (table == null) {
            throw new IOException("Unsupported account platform: " + platform);
        }
        return table;
    }

    private String normalizeAccountKey(String accountKey) throws IOException {
        String normalized = TextSupport.text(accountKey);
        if (normalized.isBlank()) {
            throw new IOException("Missing account key");
        }
        return normalized;
    }

    private String minioUrl(String objectKey) {
        return minioEndpoint + "/" + minioBucket + "/" + objectKey;
    }

    private static String trimTrailingSlash(String value) {
        String text = TextSupport.text(value);
        while (text.endsWith("/")) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }
}
