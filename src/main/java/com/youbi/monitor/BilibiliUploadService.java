package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class BilibiliUploadService {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String USER_AGENT = "Mozilla/5.0 BiliDroid/7.80.0 (bbcallen@gmail.com) os/android model/MI 6 mobi_app/android build/7800300 channel/bili innerVer/7800310 osVer/13 network/2";

    private final BilibiliAccountService accountService;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final MinioClient minioClient;
    private final String minioBucket;
    private final Path uploadWorkDir;

    public BilibiliUploadService(
            BilibiliAccountService accountService,
            ObjectMapper objectMapper,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket,
            @Value("${youbi.minio.work-dir}") String uploadWorkDir
    ) {
        this.accountService = accountService;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(60))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.uploadWorkDir = Path.of(uploadWorkDir).toAbsolutePath().normalize();
    }

    public BilibiliUploadResult upload(BilibiliUploadRequest request) throws IOException, InterruptedException {
        ResolvedVideo resolvedVideo = resolveVideo(request);
        try {
            Path videoPath = resolvedVideo.path();
            if (!Files.isRegularFile(videoPath) || Files.size(videoPath) == 0) {
                throw new IOException("Video file does not exist or is empty: " + videoPath);
            }

            String accountKey = accountService.normalizeAccountKey(request.accountKey());
            JsonNode loginInfo = accountService.loginInfo(accountKey);
            String cookie = accountService.cookieHeader(loginInfo);
            String accessToken = accountService.accessToken(loginInfo);
            UploadedVideo uploadedVideo = uploadVideoFile(videoPath, request, cookie);
            JsonNode submit = submit(uploadedVideo, request, accessToken, cookie);
            int code = submit.path("code").asInt(Integer.MIN_VALUE);
            Map<String, Object> raw = objectMapper.convertValue(submit, MAP_TYPE);
            if (code != 0) {
                return new BilibiliUploadResult(false, "", null, submit.path("message").asText(submit.toString()), raw);
            }
            JsonNode data = submit.path("data");
            String bvid = data.path("bvid").asText("");
            Long aid = data.path("aid").canConvertToLong() ? data.path("aid").asLong() : null;
            return new BilibiliUploadResult(true, bvid, aid, "上传成功", raw);
        } finally {
            if (resolvedVideo.temporary()) {
                Files.deleteIfExists(resolvedVideo.path());
            }
        }
    }

    private ResolvedVideo resolveVideo(BilibiliUploadRequest request) throws IOException {
        String minioUrl = firstText(request.videoUrl(), request.minioUrl());
        if (!minioUrl.isBlank()) {
            return new ResolvedVideo(downloadMinioVideo(minioUrl, request.taskId()), true);
        }

        Path videoPath = Path.of(required(request.videoPath(), "videoPath")).toAbsolutePath().normalize();
        return new ResolvedVideo(videoPath, false);
    }

    private Path downloadMinioVideo(String minioUrl, String taskId) throws IOException {
        ObjectRef objectRef = parseObjectRef(minioUrl);
        String filename = sanitizeFilename(Path.of(objectRef.objectName()).getFileName().toString());
        Path taskDir = uploadWorkDir.resolve(safeSegment(firstText(taskId, "manual"))).resolve(UUID.randomUUID().toString());
        Path destination = taskDir.resolve(filename);
        Files.createDirectories(taskDir);
        try (InputStream input = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(objectRef.bucket())
                        .object(objectRef.objectName())
                        .build()
        )) {
            Files.copy(input, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception exc) {
            throw new IOException("Cannot download MinIO video: " + minioUrl, exc);
        }
        if (!Files.isRegularFile(destination) || Files.size(destination) == 0) {
            throw new IOException("Downloaded MinIO video is empty: " + minioUrl);
        }
        return destination;
    }

    private ObjectRef parseObjectRef(String ref) throws IOException {
        String value = text(ref);
        if (value.isBlank()) {
            throw new IOException("Missing field: videoUrl");
        }

        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException exc) {
            throw new IOException("Invalid MinIO video URL: " + ref, exc);
        }
        if ("s3".equals(uri.getScheme())) {
            String objectName = decode(uri.getPath()).replaceFirst("^/+", "");
            String bucket = text(uri.getHost()).isBlank() ? minioBucket : uri.getHost();
            return requiredObjectRef(bucket, objectName, ref);
        }
        if ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) {
            return requiredObjectRef(minioBucket, stripKnownPrefix(decode(uri.getPath())), ref);
        }
        if (value.startsWith("/minio/") || value.startsWith("/" + minioBucket + "/") || value.startsWith(minioBucket + "/")) {
            return requiredObjectRef(minioBucket, stripKnownPrefix(value), ref);
        }
        throw new IOException("Unsupported MinIO video URL: " + ref);
    }

    private ObjectRef requiredObjectRef(String bucket, String objectName, String ref) throws IOException {
        String cleanObjectName = text(objectName).replaceFirst("^/+", "");
        if (cleanObjectName.isBlank()) {
            throw new IOException("Cannot resolve MinIO object from videoUrl: " + ref);
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

    private UploadedVideo uploadVideoFile(Path videoPath, BilibiliUploadRequest request, String cookie) throws IOException, InterruptedException {
        JsonNode bucket = preUpload(videoPath, request, cookie);
        int chunkSize = Math.max(1, bucket.path("chunk_size").asInt(10 * 1024 * 1024));
        String auth = required(bucket.path("auth").asText(""), "preupload.auth");
        String endpoint = required(bucket.path("endpoint").asText(""), "preupload.endpoint");
        String uposUri = required(bucket.path("upos_uri").asText(""), "preupload.upos_uri");
        long bizId = bucket.path("biz_id").asLong();
        String url = "https:" + endpoint + "/" + uposUri.replaceFirst("^upos://", "");
        String uploadId = initUpload(url, auth, cookie);

        long totalSize = Files.size(videoPath);
        long chunks = Math.max(1, (long) Math.ceil((double) totalSize / (double) chunkSize));
        List<Map<String, Object>> parts = new ArrayList<>();
        try (InputStream input = Files.newInputStream(videoPath)) {
            for (int index = 0; index < chunks; index++) {
                byte[] bytes = input.readNBytes((int) Math.min(chunkSize, totalSize - (long) index * chunkSize));
                if (bytes.length == 0) {
                    break;
                }
                uploadChunk(url, auth, cookie, uploadId, chunks, totalSize, (long) index * chunkSize, index, bytes);
                parts.add(Map.of("partNumber", index + 1, "eTag", "etag"));
            }
        }

        JsonNode completed = completeUpload(url, auth, cookie, videoPath.getFileName().toString(), uploadId, bizId, parts);
        if (completed.path("OK").asInt(0) != 1) {
            throw new IOException("Bilibili upload complete failed: " + completed);
        }
        return new UploadedVideo(partTitle(videoPath), filenameFromUpos(uposUri), "");
    }

    private JsonNode preUpload(Path videoPath, BilibiliUploadRequest request, String cookie) throws IOException, InterruptedException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("name", videoPath.getFileName().toString());
        params.put("r", "upos");
        params.put("profile", "ugcupos/bup");
        params.put("ssl", "0");
        params.put("version", "2.14.0");
        params.put("build", "2140000");
        params.put("size", String.valueOf(Files.size(videoPath)));
        String lineQuery = lineQuery(request.line());
        URI uri = URI.create("https://member.bilibili.com/preupload?" + lineQuery + "&" + accountService.encodeForm(params));
        HttpRequest httpRequest = baseRequest(uri, cookie).GET().build();
        HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Bilibili preupload failed: " + response.body());
        }
        return objectMapper.readTree(response.body());
    }

    private String initUpload(String url, String auth, String cookie) throws IOException, InterruptedException {
        HttpRequest request = baseRequest(URI.create(url + "?uploads&output=json"), cookie)
                .header("X-Upos-Auth", auth)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        JsonNode root = sendJson(request);
        String uploadId = root.path("upload_id").asText("");
        if (uploadId.isBlank()) {
            throw new IOException("Bilibili init upload returned no upload_id: " + root);
        }
        return uploadId;
    }

    private void uploadChunk(String url, String auth, String cookie, String uploadId, long chunks, long total, long start, int index, byte[] bytes) throws IOException, InterruptedException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("uploadId", uploadId);
        params.put("chunks", String.valueOf(chunks));
        params.put("total", String.valueOf(total));
        params.put("chunk", String.valueOf(index));
        params.put("size", String.valueOf(bytes.length));
        params.put("partNumber", String.valueOf(index + 1));
        params.put("start", String.valueOf(start));
        params.put("end", String.valueOf(start + bytes.length));
        HttpRequest request = baseRequest(URI.create(url + "?" + accountService.encodeForm(params)), cookie)
                .header("X-Upos-Auth", auth)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(bytes))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Bilibili chunk upload failed: " + response.statusCode() + " " + response.body());
        }
    }

    private JsonNode completeUpload(String url, String auth, String cookie, String fileName, String uploadId, long bizId, List<Map<String, Object>> parts) throws IOException, InterruptedException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("name", fileName);
        params.put("uploadId", uploadId);
        params.put("biz_id", String.valueOf(bizId));
        params.put("output", "json");
        params.put("profile", "ugcupos/bup");
        HttpRequest request = baseRequest(URI.create(url + "?" + accountService.encodeForm(params)), cookie)
                .header("X-Upos-Auth", auth)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(Map.of("parts", parts)), StandardCharsets.UTF_8))
                .build();
        return sendJson(request);
    }

    private JsonNode submit(UploadedVideo uploadedVideo, BilibiliUploadRequest request, String accessToken, String cookie) throws IOException, InterruptedException {
        Map<String, String> query = accountService.signedTvQuery(Map.of(
                "access_key", accessToken,
                "build", "7800300",
                "c_locale", "zh-Hans_CN",
                "channel", "bili",
                "disable_rcmd", "0",
                "mobi_app", "android",
                "platform", "android",
                "s_locale", "zh-Hans_CN",
                "statistics", "\"appId\":1,\"platform\":3,\"version\":\"7.80.0\",\"abtest\":\"\""
        ));
        Map<String, Object> studio = new LinkedHashMap<>();
        studio.put("copyright", request.copyright() == null ? 2 : request.copyright());
        studio.put("source", text(request.source()));
        studio.put("tid", request.tid() == null ? 171 : request.tid());
        studio.put("cover", "");
        studio.put("title", truncate(required(request.title(), "title"), 80));
        studio.put("desc_format_id", 0);
        studio.put("desc", text(request.description()));
        studio.put("desc_v2", List.of(Map.of("raw_text", text(request.description()), "biz_id", "", "type", 1)));
        studio.put("dynamic", text(request.dynamic()));
        studio.put("subtitle", Map.of("open", 0, "lan", ""));
        studio.put("tag", text(request.tags()));
        studio.put("videos", List.of(Map.of("title", uploadedVideo.title(), "filename", uploadedVideo.filename(), "desc", uploadedVideo.description())));
        studio.put("open_subtitle", false);
        studio.put("interactive", 0);
        studio.put("dolby", 0);
        studio.put("lossless_music", 0);
        studio.put("no_reprint", request.noReprint() == null ? 1 : request.noReprint());
        studio.put("charging_pay", 0);
        studio.put("up_selection_reply", false);
        studio.put("up_close_reply", false);
        studio.put("up_close_danmu", false);

        HttpRequest httpRequest = baseRequest(
                URI.create("https://member.bilibili.com/x/vu/app/add?" + accountService.encodeForm(query)),
                cookie
        )
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(studio), StandardCharsets.UTF_8))
                .build();
        return sendJson(httpRequest);
    }

    private HttpRequest.Builder baseRequest(URI uri, String cookie) {
        return HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMinutes(5))
                .header("User-Agent", USER_AGENT)
                .header("Referer", "https://www.bilibili.com/")
                .header("Cookie", cookie);
    }

    private JsonNode sendJson(HttpRequest request) throws IOException, InterruptedException {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Bilibili request failed: " + response.statusCode() + " " + response.body());
        }
        return objectMapper.readTree(response.body());
    }

    private String lineQuery(String line) {
        return switch (text(line)) {
            case "cntx" -> "zone=cs&upcdn=cntx&probe_version=20221109";
            case "bldsa" -> "zone=cs&upcdn=bldsa&probe_version=20221109";
            case "cnbldsa" -> "zone=cs&upcdn=cnbldsa&probe_version=20221109";
            case "bda2" -> "probe_version=20221109&upcdn=bda2&zone=cs";
            case "cnbd" -> "probe_version=20221109&upcdn=cnbd&zone=cs";
            case "txa" -> "zone=cs&upcdn=txa&probe_version=20221109";
            case "alia" -> "zone=cs&upcdn=alia&probe_version=20221109";
            default -> "zone=cs&upcdn=tx&probe_version=20221109";
        };
    }

    private String filenameFromUpos(String uposUri) {
        String name = Path.of(uposUri.replaceFirst("^upos://", "")).getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    private String partTitle(Path path) {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return truncate(dot > 0 ? name.substring(0, dot) : name, 80);
    }

    private String truncate(String value, int max) {
        if (value.codePointCount(0, value.length()) <= max) {
            return value;
        }
        return value.codePoints()
                .limit(max - 3L)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .append("...")
                .toString();
    }

    private String text(String value) {
        return value == null ? "" : value.trim();
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

    private String decode(String value) {
        return URLDecoder.decode(text(value), StandardCharsets.UTF_8);
    }

    private String sanitizeFilename(String value) {
        String sanitized = text(value).replaceAll("[\\\\/:*?\"<>|]+", "_");
        return sanitized.isBlank() ? "video.mp4" : sanitized;
    }

    private String safeSegment(String value) {
        String sanitized = text(value).replaceAll("[^A-Za-z0-9._-]+", "_");
        return sanitized.isBlank() ? "manual" : sanitized;
    }

    private String required(String value, String field) throws IOException {
        String text = text(value);
        if (text.isBlank()) {
            throw new IOException("Missing field: " + field);
        }
        return text;
    }

    private record ObjectRef(String bucket, String objectName) {
    }

    private record ResolvedVideo(Path path, boolean temporary) {
    }

    private record UploadedVideo(String title, String filename, String description) {
    }
}
