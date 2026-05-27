package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.math.ec.FixedPointCombMultiplier;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.Security;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class AliDriveService {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String API = "https://api.aliyundrive.com/";
    private static final String ORIGIN = "https://www.aliyundrive.com";
    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";
    private static final String APP_ID = "5dde4e1bdf9e4966b387ba58f4b3fdc3";
    private static final int CHUNK_SIZE = 10 * 1024 * 1024;
    private static final String ACCOUNT_TABLE = "uploader_account_alidrive";
    private static final String LEGACY_ACCOUNT_TABLE = "yd_alidrive_account";

    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final HttpClient httpClient;
    private final String accountKey;
    private final Path workDir;
    private final SecureRandom secureRandom = new SecureRandom();

    private String refreshToken;
    private String accessToken = "";
    private String tokenType = "Bearer";
    private Instant expireTime = Instant.EPOCH;
    private String userId = "";
    private String userName = "";
    private String nickName = "";
    private String deviceId = "";
    private String defaultDriveId = "";
    private String signature = "";
    private int nonce = 0;

    public AliDriveService(
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            @Value("${youbi.alidrive.account-key}") String accountKey,
            @Value("${youbi.alidrive.refresh-token}") String refreshToken,
            @Value("${youbi.alidrive.work-dir}") String workDir
    ) {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.accountKey = firstText(accountKey, "default");
        this.workDir = Path.of(workDir).toAbsolutePath().normalize();
        ensureSchema();
        this.refreshToken = initialRefreshToken(refreshToken);
    }

    public Map<String, Object> me() throws IOException, InterruptedException {
        ensureToken();
        return Map.of(
                "userId", userId,
                "userName", userName,
                "nickName", nickName,
                "defaultDriveId", defaultDriveId,
                "expireTime", expireTime.toString()
        );
    }

    public Map<String, Object> list(String path) throws IOException, InterruptedException {
        String normalizedPath = normalizeRemotePath(path);
        String parentFileId = "root";
        if (!"/".equals(normalizedPath)) {
            JsonNode dir = metaByPath(normalizedPath);
            if (!"folder".equals(dir.path("type").asText())) {
                throw new IOException("AliDrive path is not a folder: " + path);
            }
            parentFileId = dir.path("file_id").asText();
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("all", false);
        body.put("drive_id", defaultDriveId());
        body.put("fields", "*");
        body.put("limit", 200);
        body.put("order_by", "name");
        body.put("order_direction", "ASC");
        body.put("parent_file_id", parentFileId);
        body.put("url_expire_sec", 14400);
        body.put("image_thumbnail_process", "image/resize,w_256/format,jpeg");
        body.put("image_url_process", "image/resize,w_1920/format,jpeg/interlace,1");
        body.put("video_thumbnail_process", "video/snapshot,t_1000,f_jpg,ar_auto,w_256");
        JsonNode root = postJson("adrive/v3/file/list", body, true);
        return objectMapper.convertValue(root, MAP_TYPE);
    }

    public AliDriveTransferResult upload(AliDriveUploadRequest request) throws IOException, InterruptedException {
        Path localPath = Path.of(required(request.localPath(), "localPath")).toAbsolutePath().normalize();
        if (!Files.isRegularFile(localPath) || Files.size(localPath) == 0) {
            throw new IOException("Local file does not exist or is empty: " + localPath);
        }

        String remotePath = text(request.remotePath());
        String remoteDir = text(request.remoteDir());
        String filename;
        if (!remotePath.isBlank()) {
            remotePath = normalizeRemotePath(remotePath);
            int split = remotePath.lastIndexOf('/');
            remoteDir = split <= 0 ? "/" : remotePath.substring(0, split);
            filename = remotePath.substring(split + 1);
        } else {
            remoteDir = remoteDir.isBlank() ? "/" : normalizeRemotePath(remoteDir);
            filename = localPath.getFileName().toString();
            remotePath = joinRemote(remoteDir, filename);
        }
        String parentFileId = ensureFolder(remoteDir);
        long size = Files.size(localPath);
        int partNumber = Math.max(1, (int) Math.ceil((double) size / CHUNK_SIZE));
        JsonNode prepared = createFile(filename, parentFileId, size, partNumber, firstText(request.checkNameMode(), "auto_rename"));
        String fileId = required(prepared.path("file_id").asText(""), "file_id");
        String uploadId = required(prepared.path("upload_id").asText(""), "upload_id");
        List<String> uploadUrls = uploadUrls(prepared);
        if (uploadUrls.size() != partNumber) {
            throw new IOException("AliDrive returned " + uploadUrls.size() + " upload URLs, expected " + partNumber);
        }

        try (InputStream input = Files.newInputStream(localPath)) {
            for (int i = 0; i < partNumber; i++) {
                byte[] bytes = input.readNBytes((int) Math.min(CHUNK_SIZE, size - (long) i * CHUNK_SIZE));
                putUploadSlice(uploadUrls.get(i), bytes);
            }
        }

        JsonNode completed = postJson("v2/file/complete", Map.of(
                "drive_id", defaultDriveId(),
                "file_id", fileId,
                "upload_id", uploadId
        ), false);
        return new AliDriveTransferResult(true, "uploaded", fileId, completed.path("name").asText(filename), remotePath, localPath.toString(), size, safeRaw(completed));
    }

    public AliDriveTransferResult download(AliDriveDownloadRequest request) throws IOException, InterruptedException {
        JsonNode meta;
        if (!text(request.fileId()).isBlank()) {
            meta = metaByFileId(request.fileId());
        } else {
            meta = metaByPath(normalizeRemotePath(required(request.remotePath(), "remotePath")));
        }
        if (!"file".equals(meta.path("type").asText())) {
            throw new IOException("AliDrive target is not a file: " + meta.path("name").asText());
        }

        String fileId = required(meta.path("file_id").asText(""), "file_id");
        String downloadUrl = downloadUrl(fileId);
        Path destination = resolveDownloadDestination(request, meta.path("name").asText(fileId));
        Files.createDirectories(destination.getParent());

        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(downloadUrl))
                .timeout(Duration.ofMinutes(30))
                .header("User-Agent", USER_AGENT)
                .header("Referer", ORIGIN + "/")
                .GET()
                .build();
        HttpResponse<Path> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofFile(destination));
        if (response.statusCode() / 100 != 2) {
            Files.deleteIfExists(destination);
            throw new IOException("AliDrive download failed: HTTP " + response.statusCode());
        }
        return new AliDriveTransferResult(true, "downloaded", fileId, meta.path("name").asText(), meta.path("path").asText(text(request.remotePath())), destination.toString(), Files.size(destination), safeRaw(meta));
    }

    private JsonNode createFile(String filename, String parentFileId, long size, int partNumber, String checkNameMode) throws IOException, InterruptedException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("drive_id", defaultDriveId());
        body.put("part_info_list", partInfoList(partNumber));
        body.put("parent_file_id", parentFileId);
        body.put("name", filename);
        body.put("type", "file");
        body.put("check_name_mode", checkNameMode);
        body.put("size", size);
        body.put("pre_hash", "");
        body.put("content_hash", "");
        body.put("content_hash_name", "sha1");
        body.put("proof_code", "");
        body.put("proof_version", "v1");
        return postJson("adrive/v2/file/createWithFolders", body, false);
    }

    private String ensureFolder(String remoteDir) throws IOException, InterruptedException {
        String normalized = normalizeRemotePath(remoteDir);
        if ("/".equals(normalized)) {
            return "root";
        }
        Optional<JsonNode> existing = maybeMetaByPath(normalized);
        if (existing.isPresent()) {
            JsonNode node = existing.get();
            if (!"folder".equals(node.path("type").asText())) {
                throw new IOException("AliDrive path exists but is not a folder: " + normalized);
            }
            return node.path("file_id").asText();
        }

        String parentPath = normalized.substring(0, normalized.lastIndexOf('/'));
        if (parentPath.isBlank()) {
            parentPath = "/";
        }
        String parentFileId = ensureFolder(parentPath);
        String name = normalized.substring(normalized.lastIndexOf('/') + 1);
        JsonNode created = postJson("adrive/v2/file/createWithFolders", Map.of(
                "drive_id", defaultDriveId(),
                "parent_file_id", parentFileId,
                "name", name,
                "type", "folder",
                "check_name_mode", "refuse"
        ), false);
        return created.path("file_id").asText();
    }

    private JsonNode metaByPath(String path) throws IOException, InterruptedException {
        return maybeMetaByPath(path).orElseThrow(() -> new IOException("AliDrive path does not exist: " + path));
    }

    private Optional<JsonNode> maybeMetaByPath(String path) throws IOException, InterruptedException {
        try {
            JsonNode root = postJson("v2/file/get_by_path", Map.of(
                    "file_path", path,
                    "fields", "*",
                    "drive_id", defaultDriveId(),
                    "image_thumbnail_process", "image/resize,w_400/format,jpeg",
                    "image_url_process", "image/resize,w_375/format,jpeg",
                    "video_thumbnail_process", "video/snapshot,t_1000,f_jpg,ar_auto,w_375"
            ), true);
            return Optional.of(root);
        } catch (IOException exc) {
            if (exc.getMessage() != null && exc.getMessage().contains("NotFound.File")) {
                return Optional.empty();
            }
            throw exc;
        }
    }

    private JsonNode metaByFileId(String fileId) throws IOException, InterruptedException {
        return postJson("v2/file/get", Map.of(
                "file_id", fileId,
                "fields", "*",
                "drive_id", defaultDriveId(),
                "image_thumbnail_process", "image/resize,w_400/format,jpeg",
                "image_url_process", "image/resize,w_375/format,jpeg",
                "video_thumbnail_process", "video/snapshot,t_1000,f_jpg,ar_auto,w_375"
        ), true);
    }

    private String downloadUrl(String fileId) throws IOException, InterruptedException {
        JsonNode root = postJson("v2/file/get_download_url", Map.of(
                "drive_id", defaultDriveId(),
                "file_id", fileId
        ), true);
        return required(firstText(root.path("url").asText(""), root.path("download_url").asText("")), "download_url");
    }

    private void putUploadSlice(String uploadUrl, byte[] bytes) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(uploadUrl))
                .timeout(Duration.ofMinutes(15))
                .header("Origin", ORIGIN)
                .header("Referer", ORIGIN + "/")
                .header("User-Agent", USER_AGENT)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(bytes))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() / 100 != 2) {
            throw new IOException("AliDrive upload slice failed: HTTP " + response.statusCode() + " " + response.body());
        }
    }

    private JsonNode postJson(String node, Map<String, ?> body, boolean signed) throws IOException, InterruptedException {
        ensureToken();
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(API + node))
                .timeout(Duration.ofSeconds(60))
                .header("Origin", ORIGIN)
                .header("Referer", ORIGIN + "/")
                .header("User-Agent", USER_AGENT)
                .header("x-canary", "client=web,app=adrive,version=v4.1.0")
                .header("x-device-id", deviceId)
                .header("Authorization", tokenType + " " + accessToken)
                .header("Content-Type", "application/json;charset=UTF-8");
        if (signed) {
            builder.header("x-signature", signature());
        }
        HttpRequest request = builder.POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body), StandardCharsets.UTF_8)).build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        JsonNode root = readJson(response.body());
        if (response.statusCode() / 100 != 2 || root.hasNonNull("code")) {
            throw new IOException("AliDrive API failed: " + response.statusCode() + " " + response.body());
        }
        return root;
    }

    private synchronized void ensureToken() throws IOException, InterruptedException {
        if (!accessToken.isBlank() && Instant.now().plus(Duration.ofHours(1)).isBefore(expireTime) && !deviceId.isBlank()) {
            return;
        }
        String storedRefreshToken = loadRefreshTokenFromDb();
        if (!storedRefreshToken.isBlank()) {
            refreshToken = storedRefreshToken;
        }
        if (text(refreshToken).isBlank()) {
            throw new IOException("Missing AliDrive refresh token in uploader_account_alidrive. Run scripts/update_alidrive_refresh_token_from_chrome.zsh first.");
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(API + "token/refresh"))
                .timeout(Duration.ofSeconds(30))
                .header("Origin", ORIGIN)
                .header("Referer", ORIGIN + "/")
                .header("User-Agent", USER_AGENT)
                .header("x-canary", "client=web,app=adrive,version=v4.1.0")
                .header("Content-Type", "application/json;charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(Map.of("refresh_token", refreshToken)), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        JsonNode root = readJson(response.body());
        if (response.statusCode() / 100 != 2 || root.hasNonNull("code")) {
            throw new IOException("AliDrive token refresh failed: " + response.statusCode() + " " + response.body());
        }

        userId = root.path("user_id").asText("");
        userName = root.path("user_name").asText("");
        nickName = root.path("nick_name").asText("");
        refreshToken = root.path("refresh_token").asText(refreshToken);
        accessToken = root.path("access_token").asText("");
        tokenType = root.path("token_type").asText("Bearer");
        expireTime = Instant.parse(root.path("expire_time").asText());
        deviceId = root.path("device_id").asText("");
        defaultDriveId = root.path("default_drive_id").asText("");
        signature = "";
        persistRefreshToken();
    }

    private synchronized String signature() throws IOException, InterruptedException {
        if (!signature.isBlank()) {
            return signature;
        }
        ensureToken();
        SignPair signPair = sign(APP_ID + ":" + deviceId + ":" + userId + ":" + nonce);
        JsonNode root = postJson("users/v1/users/device/create_session", Map.of(
                "deviceName", "Chrome浏览器",
                "modelName", "Mac OS网页版",
                "pubKey", signPair.publicKey()
        ), signPair.signature());
        if (!root.path("result").asBoolean(false) || !root.path("success").asBoolean(false)) {
            throw new IOException("AliDrive create session failed: " + root);
        }
        signature = signPair.signature();
        return signature;
    }

    private JsonNode postJson(String node, Map<String, ?> body, String explicitSignature) throws IOException, InterruptedException {
        ensureToken();
        HttpRequest request = HttpRequest.newBuilder(URI.create(API + node))
                .timeout(Duration.ofSeconds(60))
                .header("Origin", ORIGIN)
                .header("Referer", ORIGIN + "/")
                .header("User-Agent", USER_AGENT)
                .header("x-canary", "client=web,app=adrive,version=v4.1.0")
                .header("x-device-id", deviceId)
                .header("x-signature", explicitSignature)
                .header("Authorization", tokenType + " " + accessToken)
                .header("Content-Type", "application/json;charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        JsonNode root = readJson(response.body());
        if (response.statusCode() / 100 != 2 || root.hasNonNull("code")) {
            throw new IOException("AliDrive API failed: " + response.statusCode() + " " + response.body());
        }
        return root;
    }

    private SignPair sign(String message) {
        X9ECParameters params = org.bouncycastle.asn1.sec.SECNamedCurves.getByName("secp256k1");
        ECDomainParameters domain = new ECDomainParameters(params.getCurve(), params.getG(), params.getN(), params.getH());
        BigInteger privateKey;
        do {
            privateKey = new BigInteger(params.getN().bitLength(), secureRandom);
        } while (privateKey.signum() <= 0 || privateKey.compareTo(params.getN()) >= 0);

        byte[] digest = sha1(message.getBytes(StandardCharsets.UTF_8));
        ECDSASigner signer = new ECDSASigner();
        signer.init(true, new ECPrivateKeyParameters(privateKey, domain));
        BigInteger[] components = signer.generateSignature(digest);
        byte[] rawSignature = ByteBuffer.allocate(65)
                .put(fixed(components[0], 32))
                .put(fixed(components[1], 32))
                .put((byte) 0)
                .array();
        byte[] publicKey = new FixedPointCombMultiplier().multiply(params.getG(), privateKey).normalize().getEncoded(false);
        byte[] rawPublicKey = new byte[64];
        System.arraycopy(publicKey, 1, rawPublicKey, 0, rawPublicKey.length);
        return new SignPair(Hex.toHexString(rawSignature), Hex.toHexString(rawPublicKey));
    }

    private static byte[] sha1(byte[] data) {
        SHA1Digest digest = new SHA1Digest();
        digest.update(data, 0, data.length);
        byte[] out = new byte[digest.getDigestSize()];
        digest.doFinal(out, 0);
        return out;
    }

    private static byte[] fixed(BigInteger value, int size) {
        byte[] bytes = value.toByteArray();
        byte[] out = new byte[size];
        int copy = Math.min(bytes.length, size);
        System.arraycopy(bytes, bytes.length - copy, out, size - copy, copy);
        return out;
    }

    private JsonNode readJson(String body) throws IOException {
        return objectMapper.readTree(text(body).isBlank() ? "{}" : body);
    }

    private String defaultDriveId() throws IOException, InterruptedException {
        ensureToken();
        return required(defaultDriveId, "default_drive_id");
    }

    private Path resolveDownloadDestination(AliDriveDownloadRequest request, String filename) throws IOException {
        String localPath = text(request.localPath());
        if (!localPath.isBlank()) {
            Path path = Path.of(localPath).toAbsolutePath().normalize();
            if (Files.isDirectory(path)) {
                return path.resolve(filename);
            }
            return path;
        }
        String outDir = text(request.outDir());
        Path dir = outDir.isBlank() ? workDir : Path.of(outDir).toAbsolutePath().normalize();
        return dir.resolve(filename);
    }

    private List<Map<String, Integer>> partInfoList(int partNumber) {
        List<Map<String, Integer>> parts = new ArrayList<>();
        for (int i = 1; i <= partNumber; i++) {
            parts.add(Map.of("part_number", i));
        }
        return parts;
    }

    private List<String> uploadUrls(JsonNode prepared) {
        List<String> urls = new ArrayList<>();
        for (JsonNode part : prepared.path("part_info_list")) {
            String url = part.path("upload_url").asText("");
            if (!url.isBlank()) {
                urls.add(url);
            }
        }
        return urls;
    }

    private Map<String, Object> safeRaw(JsonNode node) {
        Map<String, Object> raw = objectMapper.convertValue(node, MAP_TYPE);
        raw.remove("url");
        raw.remove("download_url");
        raw.remove("thumbnail");
        return raw;
    }

    private String initialRefreshToken(String configured) {
        String value = text(configured);
        if (!value.isBlank()) {
            refreshToken = value;
            persistRefreshToken();
            return value;
        }
        return loadRefreshTokenFromDb();
    }

    private void persistRefreshToken() {
        if (text(refreshToken).isBlank()) {
            return;
        }
        jdbcTemplate.update("""
                INSERT INTO uploader_account_alidrive (
                    account_key, refresh_token, user_id, user_name, nick_name, default_drive_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    refresh_token = VALUES(refresh_token),
                    user_id = VALUES(user_id),
                    user_name = VALUES(user_name),
                    nick_name = VALUES(nick_name),
                    default_drive_id = VALUES(default_drive_id),
                    updated_at = CURRENT_TIMESTAMP
                """, accountKey, refreshToken, userId, userName, nickName, defaultDriveId);
    }

    private String loadRefreshTokenFromDb() {
        List<String> tokens = jdbcTemplate.query(
                "SELECT refresh_token FROM uploader_account_alidrive WHERE account_key = ? LIMIT 1",
                (rs, rowNum) -> rs.getString("refresh_token"),
                accountKey
        );
        return tokens.isEmpty() ? "" : text(tokens.get(0));
    }

    private void ensureSchema() {
        if (!tableExists(ACCOUNT_TABLE) && tableExists(LEGACY_ACCOUNT_TABLE)) {
            jdbcTemplate.execute("RENAME TABLE yd_alidrive_account TO uploader_account_alidrive");
        }
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS uploader_account_alidrive (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    refresh_token TEXT NOT NULL,
                    user_id VARCHAR(128) NULL,
                    user_name VARCHAR(128) NULL,
                    nick_name VARCHAR(255) NULL,
                    default_drive_id VARCHAR(128) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);
    }

    private boolean tableExists(String tableName) {
        Integer count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = DATABASE()
                            AND table_name = ?
                        """,
                Integer.class,
                tableName
        );
        return count != null && count > 0;
    }

    private static String normalizeRemotePath(String path) {
        String value = text(path);
        if (value.isBlank() || "/".equals(value)) {
            return "/";
        }
        value = value.replace('\\', '/');
        if (!value.startsWith("/")) {
            value = "/" + value;
        }
        while (value.contains("//")) {
            value = value.replace("//", "/");
        }
        if (value.length() > 1 && value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String joinRemote(String dir, String name) {
        String normalizedDir = normalizeRemotePath(dir);
        return "/".equals(normalizedDir) ? "/" + name : normalizedDir + "/" + name;
    }

    private static String required(String value, String name) throws IOException {
        String text = text(value);
        if (text.isBlank()) {
            throw new IOException("Missing field: " + name);
        }
        return text;
    }

    private static String firstText(String... values) {
        for (String value : values) {
            String text = text(value);
            if (!text.isBlank()) {
                return text;
            }
        }
        return "";
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }

    private record SignPair(String signature, String publicKey) {
    }
}
