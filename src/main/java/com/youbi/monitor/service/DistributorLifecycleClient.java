package com.youbi.monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Service
public class DistributorLifecycleClient {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;

    public DistributorLifecycleClient(
            ObjectMapper objectMapper,
            @Value("${youbi.distributor.base-url:http://127.0.0.1:8210}") String baseUrl
    ) {
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl.replaceFirst("/+$", "");
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public JsonNode stop(String taskId) {
        return post(taskId, "stop");
    }

    public JsonNode retry(String taskId) {
        return post(taskId, "retry");
    }

    public JsonNode restart(String taskId) {
        return post(taskId, "restart");
    }

    private JsonNode post(String taskId, String action) {
        String encodedTaskId = URLEncoder.encode(taskId, StandardCharsets.UTF_8).replace("+", "%20");
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/tasks/" + encodedTaskId + "/" + action))
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = response.body().isBlank() ? objectMapper.createObjectNode() : objectMapper.readTree(response.body());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(body.path("message").asText("Distributor lifecycle request failed."));
            }
            return body;
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Distributor lifecycle request interrupted.", exc);
        } catch (IOException exc) {
            throw new IllegalStateException("Cannot reach distributor lifecycle API at " + baseUrl, exc);
        }
    }
}
