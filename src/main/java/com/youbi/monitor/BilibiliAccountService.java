package com.youbi.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Service
public class BilibiliAccountService {
    static final String DEFAULT_ACCOUNT_KEY = "default";
    static final String AUTO_ACCOUNT_KEY = "_auto";
    static final String APP_KEY_BILI_TV = "4409e2ce8ffd12b8";
    static final String APP_SECRET_BILI_TV = "59b43e04ad6965f34319062b478f83dd";

    private static final String TABLE = "yd_bilibili_account";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public BilibiliAccountService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        this.objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        ensureSchema();
    }

    public BilibiliAccountStatus status() throws IOException {
        return status(DEFAULT_ACCOUNT_KEY);
    }

    public BilibiliAccountStatus status(String accountKey) throws IOException {
        return status(normalizeAccountKey(accountKey), Optional.empty());
    }

    public List<BilibiliAccountStatus> accounts() {
        return jdbcTemplate.query(
                "SELECT account_key, mid, uname, login_info_json, updated_at FROM " + TABLE + " ORDER BY account_key",
                (rs, rowNum) -> {
                    String accountKey = rs.getString("account_key");
                    String json = rs.getString("login_info_json");
                    Long mid = rs.getObject("mid") == null ? null : rs.getLong("mid");
                    LocalDateTime updatedAt = rs.getTimestamp("updated_at") == null ? null : rs.getTimestamp("updated_at").toLocalDateTime();
                    return new BilibiliAccountStatus(
                            "database",
                            accountKey,
                            json != null && !json.isBlank(),
                            json == null ? 0 : json.getBytes(StandardCharsets.UTF_8).length,
                            updatedAt,
                            mid,
                            rs.getString("uname"),
                            null,
                            null,
                            null,
                            "已保存",
                            Map.of()
                    );
                }
        );
    }

    public JsonNode loginInfo(String accountKey) throws IOException {
        String normalized = normalizeAccountKey(accountKey);
        return loadLoginInfo(normalized)
                .orElseThrow(() -> new IOException("Bilibili account is not logged in: " + normalized));
    }

    public String cookieHeader(JsonNode loginInfo) {
        List<String> cookies = new ArrayList<>();
        for (JsonNode cookie : loginInfo.path("cookie_info").path("cookies")) {
            String name = cookie.path("name").asText("");
            String value = cookie.path("value").asText("");
            if (!name.isBlank() && !value.isBlank()) {
                cookies.add(name + "=" + value);
            }
        }
        return String.join("; ", cookies);
    }

    public String accessToken(JsonNode loginInfo) throws IOException {
        return requiredText(loginInfo.path("token_info"), "access_token");
    }

    public BilibiliQrCode createQrCode(String accountKey) throws IOException, InterruptedException {
        String normalized = normalizeRequestedAccountKey(accountKey);
        Map<String, String> form = signedParams(Map.of("local_id", "0"), APP_KEY_BILI_TV, APP_SECRET_BILI_TV);
        JsonNode root = postForm("https://passport.bilibili.com/x/passport-tv-login/qrcode/auth_code", form);
        ensureCode(root, 0, "create qrcode");
        JsonNode data = root.path("data");
        String authCode = requiredText(data, "auth_code");
        String url = requiredText(data, "url");
        return new BilibiliQrCode(normalized, authCode, url, Instant.now().getEpochSecond() + 180);
    }

    public BilibiliQrPollResult pollQrCode(String accountKey, String authCode) throws IOException, InterruptedException {
        String normalized = normalizeRequestedAccountKey(accountKey);
        Map<String, String> form = signedParams(
                Map.of("auth_code", authCode, "local_id", "0"),
                APP_KEY_BILI_TV,
                APP_SECRET_BILI_TV
        );
        JsonNode root = postForm("https://passport.bilibili.com/x/passport-tv-login/qrcode/poll", form);
        int code = root.path("code").asInt(-1);
        String message = root.path("message").asText("");
        if (code == 0) {
            JsonNode data = root.path("data");
            if (data.isMissingNode() || data.isNull()) {
                throw new IOException("Bilibili qrcode poll returned no login data.");
            }
            String saveKey = AUTO_ACCOUNT_KEY.equals(normalized) ? automaticAccountKey(data) : normalized;
            saveLoginInfo(saveKey, data);
            return new BilibiliQrPollResult(true, code, "登录成功", status(saveKey, Optional.of(data)));
        }
        return new BilibiliQrPollResult(false, code, message.isBlank() ? "等待扫码确认" : message, AUTO_ACCOUNT_KEY.equals(normalized) ? emptyAutoStatus() : status(normalized));
    }

    public BilibiliAccountStatus renew() throws IOException, InterruptedException {
        return renew(DEFAULT_ACCOUNT_KEY);
    }

    public BilibiliAccountStatus renew(String accountKey) throws IOException, InterruptedException {
        String normalized = normalizeAccountKey(accountKey);
        JsonNode loginInfo = loginInfo(normalized);
        JsonNode tokenInfo = loginInfo.path("token_info");
        String accessToken = requiredText(tokenInfo, "access_token");
        String refreshToken = requiredText(tokenInfo, "refresh_token");
        Map<String, String> form = signedParams(Map.of(
                "access_key", accessToken,
                "actionKey", "appkey",
                "refresh_token", refreshToken
        ), APP_KEY_BILI_TV, APP_SECRET_BILI_TV);
        JsonNode root = postForm("https://passport.bilibili.com/x/passport-login/oauth2/refresh_token", form);
        ensureCode(root, 0, "renew token");
        JsonNode data = root.path("data");
        if (data.isMissingNode() || data.path("cookie_info").isMissingNode()) {
            throw new IOException("Bilibili renew returned no cookie info: " + root);
        }
        saveLoginInfo(normalized, data);
        return status(normalized, Optional.of(data));
    }

    public Map<String, String> signedTvQuery(Map<String, String> params) throws IOException {
        return signedParams(params, APP_KEY_BILI_TV, APP_SECRET_BILI_TV);
    }

    public BilibiliAccountStatus renameAccountKey(String oldAccountKey, String newAccountKey) throws IOException {
        String oldKey = normalizeAccountKey(oldAccountKey);
        String newKey = normalizeAccountKey(newAccountKey);
        if (oldKey.equals(newKey)) {
            return status(oldKey);
        }
        int exists = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?",
                Integer.class,
                newKey
        );
        if (exists > 0) {
            throw new IOException("Bilibili account key already exists: " + newKey);
        }
        int updated = jdbcTemplate.update(
                "UPDATE " + TABLE + " SET account_key = ?, updated_at = NOW() WHERE account_key = ?",
                newKey,
                oldKey
        );
        if (updated != 1) {
            throw new IOException("Bilibili account key not found: " + oldKey);
        }
        return status(newKey);
    }

    public String normalizeAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return DEFAULT_ACCOUNT_KEY;
        }
        if (!normalized.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid Bilibili accountKey: " + accountKey);
        }
        return normalized;
    }

    private String normalizeRequestedAccountKey(String accountKey) {
        String normalized = accountKey == null ? "" : accountKey.trim();
        if (normalized.isBlank()) {
            return AUTO_ACCOUNT_KEY;
        }
        if (AUTO_ACCOUNT_KEY.equals(normalized)) {
            return normalized;
        }
        return normalizeAccountKey(normalized);
    }

    private BilibiliAccountStatus emptyAutoStatus() {
        return new BilibiliAccountStatus("database", AUTO_ACCOUNT_KEY, false, 0, null,
                null, null, null, null, false, "等待扫码", Map.of());
    }

    private String automaticAccountKey(JsonNode loginInfo) {
        long mid = loginInfo.path("token_info").path("mid").asLong(0);
        if (mid > 0) {
            List<String> existing = jdbcTemplate.query(
                    "SELECT account_key FROM " + TABLE + " WHERE mid = ? ORDER BY updated_at DESC LIMIT 1",
                    (rs, rowNum) -> rs.getString("account_key"),
                    mid
            );
            if (!existing.isEmpty()) {
                return existing.get(0);
            }
        }
        String base = mid > 0 ? "uid_" + mid : "account";
        String candidate = base;
        for (int index = 2; accountKeyExists(candidate); index++) {
            candidate = base + "_" + index;
        }
        return candidate;
    }

    private boolean accountKeyExists(String accountKey) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE account_key = ?",
                Integer.class,
                accountKey
        );
        return count != null && count > 0;
    }

    private BilibiliAccountStatus status(String accountKey, Optional<JsonNode> knownLoginInfo) throws IOException {
        Optional<JsonNode> stored = knownLoginInfo.isPresent() ? knownLoginInfo : loadLoginInfo(accountKey);
        if (stored.isEmpty()) {
            return new BilibiliAccountStatus("database", accountKey, false, 0, null,
                    null, null, null, null, false, "未登录", Map.of());
        }

        JsonNode loginInfo = stored.get();
        String json = objectMapper.writeValueAsString(objectMapper.convertValue(loginInfo, MAP_TYPE));
        Long mid = loginInfo.path("token_info").path("mid").canConvertToLong()
                ? loginInfo.path("token_info").path("mid").asLong()
                : null;
        LocalDateTime updatedAt = accountUpdatedAt(accountKey).orElse(null);
        try {
            JsonNode myInfo = getMyInfo(loginInfo);
            int code = myInfo.path("code").asInt(-1);
            JsonNode data = myInfo.path("data");
            Map<String, Object> raw = objectMapper.convertValue(myInfo, MAP_TYPE);
            return new BilibiliAccountStatus(
                    "database",
                    accountKey,
                    true,
                    json.getBytes(StandardCharsets.UTF_8).length,
                    updatedAt,
                    data.path("mid").canConvertToLong() ? data.path("mid").asLong() : mid,
                    data.path("name").asText(null),
                    data.path("face").asText(null),
                    data.path("level").canConvertToInt() ? data.path("level").asInt() : null,
                    code == 0,
                    code == 0 ? "已登录" : myInfo.path("message").asText("账号状态异常"),
                    raw
            );
        } catch (Exception exception) {
            return new BilibiliAccountStatus("database", accountKey, true,
                    json.getBytes(StandardCharsets.UTF_8).length, updatedAt,
                    mid, null, null, null, null, exception.getMessage(), Map.of());
        }
    }

    private JsonNode getMyInfo(JsonNode loginInfo) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://api.bilibili.com/x/space/myinfo"))
                .header("User-Agent", "Mozilla/5.0")
                .header("Referer", "https://www.bilibili.com/")
                .header("Cookie", cookieHeader(loginInfo))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return objectMapper.readTree(response.body());
    }

    private JsonNode postForm(String url, Map<String, String> form) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("User-Agent", "Mozilla/5.0")
                .POST(HttpRequest.BodyPublishers.ofString(encodeForm(form), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return objectMapper.readTree(response.body());
    }

    private Optional<JsonNode> loadLoginInfo(String accountKey) throws IOException {
        List<String> values = jdbcTemplate.query(
                "SELECT login_info_json FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getString("login_info_json"),
                accountKey
        );
        if (values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            return Optional.empty();
        }
        return Optional.of(objectMapper.readTree(values.get(0)));
    }

    private Optional<LocalDateTime> accountUpdatedAt(String accountKey) {
        List<LocalDateTime> values = jdbcTemplate.query(
                "SELECT updated_at FROM " + TABLE + " WHERE account_key = ?",
                (rs, rowNum) -> rs.getTimestamp("updated_at").toLocalDateTime(),
                accountKey
        );
        return values.stream().findFirst();
    }

    private void saveLoginInfo(String accountKey, JsonNode loginInfo) throws IOException {
        Map<String, Object> value = objectMapper.convertValue(loginInfo, MAP_TYPE);
        value.putIfAbsent("platform", "BiliTV");
        JsonNode normalized = objectMapper.valueToTree(value);
        Long mid = normalized.path("token_info").path("mid").canConvertToLong()
                ? normalized.path("token_info").path("mid").asLong()
                : null;
        String uname = "";
        try {
            uname = getMyInfo(normalized).path("data").path("name").asText("");
        } catch (Exception ignored) {
            // Account metadata is best effort; login_info_json is the source of truth.
        }
        jdbcTemplate.update(
                """
                INSERT INTO yd_bilibili_account (account_key, mid, uname, login_info_json, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE mid = VALUES(mid), uname = VALUES(uname), login_info_json = VALUES(login_info_json), updated_at = NOW()
                """,
                accountKey,
                mid,
                uname,
                objectMapper.writeValueAsString(normalized)
        );
    }

    private void ensureSchema() {
        jdbcTemplate.execute(
                """
                CREATE TABLE IF NOT EXISTS yd_bilibili_account (
                    account_key VARCHAR(64) NOT NULL PRIMARY KEY,
                    mid BIGINT NULL,
                    uname VARCHAR(128) NULL,
                    login_info_json MEDIUMTEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
        );
    }

    private Map<String, String> signedParams(Map<String, String> params, String appKey, String appSecret) throws IOException {
        Map<String, String> values = new TreeMap<>(params);
        values.put("appkey", appKey);
        values.put("ts", String.valueOf(Instant.now().getEpochSecond()));
        values.put("sign", md5(encodeForm(values) + appSecret));
        return values;
    }

    private void ensureCode(JsonNode root, int expected, String operation) throws IOException {
        int code = root.path("code").asInt(Integer.MIN_VALUE);
        if (code != expected) {
            throw new IOException("Bilibili " + operation + " failed: " + root);
        }
    }

    private String requiredText(JsonNode node, String field) throws IOException {
        String value = node.path(field).asText("");
        if (value.isBlank()) {
            throw new IOException("Missing Bilibili field: " + field);
        }
        return value;
    }

    private String requiredText(String value, String field) throws IOException {
        String text = value == null ? "" : value.trim();
        if (text.isBlank()) {
            throw new IOException("Missing Bilibili field: " + field);
        }
        return text;
    }

    public String encodeForm(Map<String, String> form) {
        return form.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()))
                .reduce((left, right) -> left + "&" + right)
                .orElse("");
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String md5(String value) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : bytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception exception) {
            throw new IOException("Failed to calculate md5", exception);
        }
    }

}
