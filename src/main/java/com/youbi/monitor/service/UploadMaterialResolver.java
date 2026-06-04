package com.youbi.monitor.service;

import com.youbi.monitor.dto.AliDriveDownloadRequest;
import com.youbi.monitor.dto.AliDriveTransferResult;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.slf4j.Logger;

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
import java.util.List;
import java.util.UUID;

final class UploadMaterialResolver {
    private final MinioClient minioClient;
    private final String minioBucket;
    private final Path uploadWorkDir;
    private final HttpClient httpClient;
    private final AliDriveService aliDriveService;
    private final Logger log;
    private final String logPrefix;
    private final boolean logMinioDownloads;

    UploadMaterialResolver(
            MinioClient minioClient,
            String minioBucket,
            Path uploadWorkDir,
            HttpClient httpClient,
            AliDriveService aliDriveService,
            Logger log,
            String logPrefix,
            boolean logMinioDownloads
    ) {
        this.minioClient = minioClient;
        this.minioBucket = TextSupport.text(minioBucket).isBlank() ? "ydbi" : TextSupport.text(minioBucket);
        this.uploadWorkDir = uploadWorkDir;
        this.httpClient = httpClient;
        this.aliDriveService = aliDriveService;
        this.log = log;
        this.logPrefix = logPrefix;
        this.logMinioDownloads = logMinioDownloads;
    }

    ResolvedFile resolveVideo(String videoUrl, String minioUrl, String videoPath, String taskId) throws IOException {
        return resolveVideo("", videoUrl, minioUrl, videoPath, "", "", taskId);
    }

    ResolvedFile resolveVideo(
            String videoLocation,
            String videoUrl,
            String minioUrl,
            String videoPath,
            String alidriveFileId,
            String alidriveRemotePath,
            String taskId
    ) throws IOException {
        if (isAliDriveLocation(videoLocation) || hasAliDriveRef(alidriveFileId, alidriveRemotePath) || isAliDriveRef(videoUrl) || isAliDriveRef(minioUrl)) {
            return new ResolvedFile(downloadAliDriveFile(videoUrl, minioUrl, alidriveFileId, alidriveRemotePath, taskId), true);
        }
        String ref = TextSupport.firstText(videoUrl, minioUrl);
        if (!ref.isBlank()) {
            return new ResolvedFile(downloadMinioFile(ref, taskId, "video.mp4"), true);
        }
        return new ResolvedFile(Path.of(TextSupport.required(videoPath, "videoPath")).toAbsolutePath().normalize(), false);
    }

    ResolvedFile resolveCover(String coverPath, String coverUrl, String taskId) throws IOException {
        if (TextSupport.hasText(coverPath)) {
            return new ResolvedFile(Path.of(coverPath).toAbsolutePath().normalize(), false);
        }
        if (!TextSupport.hasText(coverUrl)) {
            return null;
        }
        String ref = TextSupport.text(coverUrl);
        if (isMinioRef(ref)) {
            return new ResolvedFile(downloadMinioFile(ref, taskId, "cover.jpg"), true);
        }
        Path taskDir = uploadWorkDir.resolve(TextSupport.safeSegment(TextSupport.firstText(taskId, "manual"))).resolve(UUID.randomUUID().toString());
        Files.createDirectories(taskDir);
        Path destination = taskDir.resolve("cover.jpg");
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(ref))
                .timeout(Duration.ofMinutes(2))
                .header("User-Agent", "Mozilla/5.0")
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() / 100 != 2 || response.body().length == 0) {
                throw new IOException("Cannot download coverUrl: " + response.statusCode() + " " + ref);
            }
            Files.write(destination, response.body());
            return new ResolvedFile(destination, true);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted downloading coverUrl", exception);
        }
    }

    void cleanupQuietly(ResolvedFile resolvedFile) {
        if (resolvedFile == null || !resolvedFile.temporary()) {
            return;
        }
        try {
            Files.deleteIfExists(resolvedFile.path());
        } catch (Exception exception) {
            log.warn("{} cleanup failed path={} message={}", logPrefix, resolvedFile.path(), exception.getMessage());
        }
    }

    void cleanupThrowing(ResolvedFile resolvedFile) throws IOException {
        if (resolvedFile != null && resolvedFile.temporary()) {
            Files.deleteIfExists(resolvedFile.path());
        }
    }

    private Path downloadMinioFile(String minioUrl, String taskId, String fallbackFilename) throws IOException {
        ObjectRef objectRef = parseObjectRef(minioUrl);
        String filename = sanitizeFilename(Path.of(objectRef.objectName()).getFileName().toString());
        if (filename.isBlank()) {
            filename = fallbackFilename;
        }
        Path taskDir = uploadWorkDir.resolve(TextSupport.safeSegment(TextSupport.firstText(taskId, "manual"))).resolve(UUID.randomUUID().toString());
        Path destination = taskDir.resolve(filename);
        Files.createDirectories(taskDir);
        long startedAt = System.currentTimeMillis();
        if (logMinioDownloads) {
            log.info("{} download minio start taskId={} bucket={} object={} destination={}", logPrefix, TextSupport.firstText(taskId, "manual"), objectRef.bucket(), objectRef.objectName(), destination);
        }
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
        if (logMinioDownloads) {
            log.info("{} download minio done taskId={} bytes={} elapsedMs={}", logPrefix, TextSupport.firstText(taskId, "manual"), Files.size(destination), System.currentTimeMillis() - startedAt);
        }
        return destination;
    }

    private Path downloadAliDriveFile(String videoUrl, String minioUrl, String fileId, String remotePath, String taskId) throws IOException {
        if (aliDriveService == null) {
            throw new IOException("AliDrive service is not configured for upload material resolver");
        }
        AliDriveRef ref = aliDriveRef(videoUrl, minioUrl, fileId, remotePath);
        if (ref.fileId().isBlank() && ref.remotePath().isBlank()) {
            throw new IOException("AliDrive video reference is missing");
        }
        Path taskDir = uploadWorkDir.resolve(TextSupport.safeSegment(TextSupport.firstText(taskId, "manual"))).resolve(UUID.randomUUID().toString());
        Files.createDirectories(taskDir);
        long startedAt = System.currentTimeMillis();
        log.info("{} download AliDrive start taskId={} fileId={} remotePath={} outDir={}", logPrefix, TextSupport.firstText(taskId, "manual"), ref.fileId(), ref.remotePath(), taskDir);
        try {
            AliDriveTransferResult result = aliDriveService.download(new AliDriveDownloadRequest(ref.remotePath(), ref.fileId(), taskDir.toString(), ""));
            Path destination = Path.of(TextSupport.required(result.localPath(), "localPath")).toAbsolutePath().normalize();
            if (!Files.isRegularFile(destination) || Files.size(destination) == 0) {
                throw new IOException("Downloaded AliDrive video is empty: " + destination);
            }
            log.info("{} download AliDrive done taskId={} fileId={} remotePath={} path={} bytes={} elapsedMs={}",
                    logPrefix, TextSupport.firstText(taskId, "manual"), result.fileId(), result.remotePath(), destination, Files.size(destination), System.currentTimeMillis() - startedAt);
            return destination;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted downloading AliDrive video", exception);
        }
    }

    private ObjectRef parseObjectRef(String ref) throws IOException {
        String value = TextSupport.text(ref);
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException exc) {
            throw new IOException("Invalid MinIO URL: " + ref, exc);
        }
        if ("s3".equals(uri.getScheme())) {
            return requiredObjectRef(TextSupport.text(uri.getHost()).isBlank() ? minioBucket : uri.getHost(), decode(uri.getPath()).replaceFirst("^/+", ""), ref);
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
        String cleanObjectName = TextSupport.text(objectName).replaceFirst("^/+", "");
        if (cleanObjectName.isBlank()) {
            throw new IOException("Cannot resolve MinIO object from URL: " + ref);
        }
        return new ObjectRef(TextSupport.text(bucket).isBlank() ? minioBucket : TextSupport.text(bucket), cleanObjectName);
    }

    private String stripKnownPrefix(String path) {
        String value = TextSupport.text(path).split("\\?", 2)[0].replaceFirst("^/+", "");
        for (String prefix : List.of("minio/" + minioBucket + "/", minioBucket + "/")) {
            if (value.startsWith(prefix)) {
                return value.substring(prefix.length());
            }
        }
        return value;
    }

    private boolean isMinioRef(String ref) {
        String value = TextSupport.text(ref);
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

    private boolean isAliDriveLocation(String value) {
        String text = TextSupport.text(value).toLowerCase();
        return text.equals("adrive") || text.equals("alidrive") || text.equals("aliyun") || text.equals("aliyundrive");
    }

    private boolean hasAliDriveRef(String fileId, String remotePath) {
        return TextSupport.hasText(fileId) || TextSupport.hasText(remotePath);
    }

    private boolean isAliDriveRef(String ref) {
        return TextSupport.text(ref).startsWith("adrive://");
    }

    private AliDriveRef aliDriveRef(String videoUrl, String minioUrl, String fileId, String remotePath) {
        String resolvedFileId = TextSupport.text(fileId);
        String resolvedRemotePath = TextSupport.text(remotePath);
        String ref = TextSupport.firstText(videoUrl, minioUrl);
        if ((resolvedFileId.isBlank() && resolvedRemotePath.isBlank()) && isAliDriveRef(ref)) {
            String value = TextSupport.text(ref).replaceFirst("^adrive://", "").trim();
            if (value.startsWith("/")) {
                resolvedRemotePath = value;
            } else {
                resolvedFileId = value;
            }
        }
        return new AliDriveRef(resolvedFileId, resolvedRemotePath);
    }

    private String decode(String value) {
        return URLDecoder.decode(TextSupport.text(value), StandardCharsets.UTF_8);
    }

    private String sanitizeFilename(String value) {
        return TextSupport.text(value).replaceAll("[\\\\/:*?\"<>|]+", "_");
    }

    record ResolvedFile(Path path, boolean temporary) {
    }

    private record ObjectRef(String bucket, String objectName) {
    }

    private record AliDriveRef(String fileId, String remotePath) {
    }
}
