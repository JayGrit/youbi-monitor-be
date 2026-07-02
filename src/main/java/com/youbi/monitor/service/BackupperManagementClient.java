package com.youbi.monitor.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

@Service
public class BackupperManagementClient {
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public BackupperManagementClient(
            @Value("${youbi.backupper.base-url:http://127.0.0.1:8219}") String baseUrl,
            ObjectMapper objectMapper
    ) {
        this.baseUrl = trimTrailingSlash(baseUrl);
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public Map<String, Object> clearBuildCache() {
        return post("/api/build-cache/clear");
    }

    public Map<String, Object> clearDiagnostics() {
        return post("/api/diagnostics/clear");
    }

    private Map<String, Object> post(String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(120))
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            Map<String, Object> payload = readPayload(response.body());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(errorMessage(payload, response.statusCode()));
            }
            return payload;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Backupper request interrupted", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Backupper request failed: " + exception.getMessage(), exception);
        }
    }

    private Map<String, Object> readPayload(String body) throws Exception {
        if (body == null || body.isBlank()) {
            return Map.of();
        }
        return objectMapper.readValue(body, new TypeReference<>() {});
    }

    private String errorMessage(Map<String, Object> payload, int statusCode) {
        Object detail = payload.get("detail");
        Object message = payload.get("message");
        return String.valueOf(detail == null ? (message == null ? "HTTP " + statusCode : message) : detail);
    }

    private static String trimTrailingSlash(String value) {
        String text = String.valueOf(value == null ? "" : value).trim();
        while (text.endsWith("/")) {
            text = text.substring(0, text.length() - 1);
        }
        return text.isBlank() ? "http://127.0.0.1:8219" : text;
    }
}
