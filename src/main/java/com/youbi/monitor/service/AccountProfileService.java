package com.youbi.monitor.service;

import com.youbi.monitor.dto.AccountProfileUpdateRequest;
import com.youbi.monitor.dto.AccountProfileUpdateResult;
import com.youbi.monitor.repository.IAccountProfileRepositoryService;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.springframework.beans.factory.annotation.Value;
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
    private static final Map<String, String> TABLES = Map.ofEntries(
            Map.entry("bilibili", "bilibili"),
            Map.entry("douyin", "douyin"),
            Map.entry("xiaohongshu", "xiaohongshu"),
            Map.entry("shipinhao", "shipinhao"),
            Map.entry("kuaishou", "kuaishou"),
            Map.entry("jinritoutiao", "jinritoutiao"),
            Map.entry("x", "x"),
            Map.entry("youtube", "youtube"),
            Map.entry("doubao", "doubao"),
            Map.entry("notebooklm", "notebooklm"),
            Map.entry("chatgpt", "chatgpt")
    );

    private final IAccountProfileRepositoryService repositoryService;
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String assetsBucket;

    public AccountProfileService(
            IAccountProfileRepositoryService repositoryService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.assets-bucket:youbi-assets}") String assetsBucket
    ) {
        this.repositoryService = repositoryService;
        this.minioEndpoint = trimTrailingSlash(TextSupport.text(minioEndpoint));
        this.assetsBucket = TextSupport.text(assetsBucket).isBlank() ? "youbi-assets" : TextSupport.text(assetsBucket);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
    }

    public AccountProfileUpdateResult updateProfile(String platform, String accountKey, AccountProfileUpdateRequest request) throws IOException {
        String table = table(platform);
        String normalized = normalizeAccountKey(accountKey);
        repositoryService.ensureProfileColumns(table);
        String displayName = TextSupport.text(request == null ? "" : request.displayName());
        int updated = repositoryService.updateDisplayName(table, normalized, displayName.isBlank() ? null : TextSupport.truncate(displayName, 128));
        if (updated < 1) {
            throw new IOException("Account key not found: " + normalized);
        }
        return repositoryService.findProfile(table, normalized);
    }

    public AccountProfileUpdateResult uploadAvatar(String platform, String accountKey, MultipartFile file) throws Exception {
        String table = table(platform);
        String normalized = normalizeAccountKey(accountKey);
        repositoryService.ensureProfileColumns(table);
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
                    .bucket(assetsBucket)
                    .object(objectKey)
                    .stream(input, file.getSize(), -1)
                    .contentType(contentType)
                    .build());
        }
        String avatarUrl = minioUrl(objectKey);
        int updated = repositoryService.updateAvatarUrl(table, normalized, avatarUrl);
        if (updated < 1) {
            throw new IOException("Account key not found: " + normalized);
        }
        return repositoryService.findProfile(table, normalized);
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
        return minioEndpoint + "/" + assetsBucket + "/" + objectKey;
    }

    private static String trimTrailingSlash(String value) {
        String text = TextSupport.text(value);
        while (text.endsWith("/")) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }
}
