package com.youbi.monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class AccountFollowerProfileSyncService {
    private static final Logger log = LoggerFactory.getLogger(AccountFollowerProfileSyncService.class);
    private static final Set<String> SUPPORTED_PLATFORMS = Set.of(
            "bilibili",
            "douyin",
            "jinritoutiao",
            "kuaishou",
            "shipinhao",
            "youtube"
    );

    private final AccountProfileService accountProfileService;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final ExecutorService executorService;
    private final String operatorApiBaseUrl;

    public AccountFollowerProfileSyncService(
            AccountProfileService accountProfileService,
            ObjectMapper objectMapper,
            @Value("${youbi.operator.api-base-url:http://127.0.0.1:8400/api}") String operatorApiBaseUrl
    ) {
        this.accountProfileService = accountProfileService;
        this.objectMapper = objectMapper;
        this.operatorApiBaseUrl = trimTrailingSlash(TextSupport.text(operatorApiBaseUrl));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.executorService = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "account-follower-profile-sync");
            thread.setDaemon(true);
            return thread;
        });
    }

    public Map<String, Object> start(String platform, String topic) throws IOException, InterruptedException {
        String normalizedPlatform = TextSupport.text(platform);
        String normalizedTopic = TextSupport.text(topic);
        if (!SUPPORTED_PLATFORMS.contains(normalizedPlatform)) {
            throw new IOException("Unsupported follower profile platform: " + platform);
        }
        if (normalizedTopic.isBlank()) {
            throw new IOException("Missing topic");
        }
        String taskId = "monitor-follower-profile-" + normalizedPlatform + "-" + normalizedTopic + "-" + UUID.randomUUID();
        String opId = submitOperatorTask(normalizedPlatform, normalizedTopic, taskId);
        executorService.submit(() -> syncFromOperatorResult(normalizedPlatform, normalizedTopic, opId));
        return Map.of(
                "accepted", true,
                "platform", normalizedPlatform,
                "topic", normalizedTopic,
                "opId", opId
        );
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdownNow();
    }

    private String submitOperatorTask(String platform, String topic, String taskId) throws IOException, InterruptedException {
        String body = objectMapper.writeValueAsString(Map.of(
                "platform", platform,
                "action", "get_follower_count",
                "topic", topic,
                "taskId", taskId,
                "payload", Map.of("topic", topic)
        ));
        JsonNode response = sendJson(
                HttpRequest.newBuilder(URI.create(operatorApiBaseUrl + "/tasks"))
                        .timeout(Duration.ofSeconds(15))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build()
        );
        String opId = TextSupport.text(response.path("opId").asText(""));
        if (opId.isBlank()) {
            throw new IOException("operator did not return opId");
        }
        return opId;
    }

    private void syncFromOperatorResult(String platform, String topic, String opId) {
        try {
            for (int attempt = 0; attempt < 30; attempt++) {
                if (attempt > 0) {
                    Thread.sleep(Duration.ofSeconds(10).toMillis());
                }
                JsonNode task = sendJson(
                        HttpRequest.newBuilder(URI.create(operatorApiBaseUrl + "/tasks/" + opId))
                                .timeout(Duration.ofSeconds(15))
                                .GET()
                                .build()
                );
                String status = TextSupport.text(task.path("status").asText(""));
                if ("success".equals(status)) {
                    JsonNode result = task.path("result");
                    String username = firstText(result, "username", "accountName");
                    String avatarUrl = firstText(result, "avatarMinioUrl", "avatar_minio_url");
                    if (username.isBlank() && avatarUrl.isBlank()) {
                        log.warn("operator follower profile result has no profile fields platform={} topic={} opId={}", platform, topic, opId);
                        return;
                    }
                    accountProfileService.applyProfileSync(platform, topic, username, avatarUrl);
                    log.info("synced follower profile platform={} topic={} opId={}", platform, topic, opId);
                    return;
                }
                if ("failed".equals(status)) {
                    log.warn(
                            "operator follower profile task failed platform={} topic={} opId={} message={}",
                            platform,
                            topic,
                            opId,
                            task.path("error").path("message").asText("")
                    );
                    return;
                }
            }
            log.warn("operator follower profile task timed out platform={} topic={} opId={}", platform, topic, opId);
        } catch (Exception exception) {
            log.warn("operator follower profile sync failed platform={} topic={} opId={}", platform, topic, opId, exception);
        }
    }

    private JsonNode sendJson(HttpRequest request) throws IOException, InterruptedException {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode payload = objectMapper.readTree(response.body() == null || response.body().isBlank() ? "{}" : response.body());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            String message = payload.path("error").path("message").asText(payload.path("message").asText("HTTP " + response.statusCode()));
            throw new IOException(message);
        }
        return payload;
    }

    private static String firstText(JsonNode node, String... fields) {
        for (String field : fields) {
            String value = TextSupport.text(node.path(field).asText(""));
            if (!value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private static String trimTrailingSlash(String value) {
        String text = TextSupport.text(value);
        while (text.endsWith("/")) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }
}
