package com.youbi.monitor.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class OperatorQueryClient {
    private final String baseUrl;
    private final Duration readTimeout;
    private final HttpClient httpClient;

    public OperatorQueryClient(
            @Value("${youbi.operator.base-url:http://127.0.0.1:8100/api}") String baseUrl,
            @Value("${youbi.operator.connect-timeout:PT3S}") Duration connectTimeout,
            @Value("${youbi.operator.read-timeout:PT15S}") Duration readTimeout
    ) {
        this.baseUrl = trimTrailingSlash(baseUrl);
        this.readTimeout = readTimeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .build();
    }

    public ResponseEntity<String> listTasks(MultiValueMap<String, String> query) {
        return get("/tasks" + queryString(query));
    }

    public ResponseEntity<String> getTask(String opId) {
        return get("/tasks/" + encode(opId));
    }

    public ResponseEntity<String> getTaskDiagnostics(String opId) {
        return get("/tasks/" + encode(opId) + "/diagnostics");
    }

    private ResponseEntity<String> get(String path) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(readTimeout)
                .GET()
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status == 400 || status == 404) {
                return json(status, response.body());
            }
            if (status >= 500) {
                return json(HttpStatus.BAD_GATEWAY.value(), errorJson("OPERATOR_QUERY_FAILED", "Operator query failed"));
            }
            return json(status, response.body());
        } catch (IOException | InterruptedException exc) {
            if (exc instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return json(HttpStatus.SERVICE_UNAVAILABLE.value(), errorJson("OPERATOR_UNAVAILABLE", "Operator is unavailable"));
        }
    }

    private static ResponseEntity<String> json(int status, String body) {
        return ResponseEntity.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body == null || body.isBlank() ? "{}" : body);
    }

    private static String queryString(MultiValueMap<String, String> query) {
        if (query == null || query.isEmpty()) {
            return "";
        }
        String value = query.entrySet().stream()
                .flatMap(entry -> values(entry).stream().map(item -> encode(entry.getKey()) + "=" + encode(item)))
                .collect(Collectors.joining("&"));
        return value.isBlank() ? "" : "?" + value;
    }

    private static List<String> values(Map.Entry<String, List<String>> entry) {
        return entry.getValue() == null ? List.of("") : entry.getValue();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
    }

    private static String trimTrailingSlash(String value) {
        String text = value == null || value.isBlank() ? "http://127.0.0.1:8100/api" : value.trim();
        while (text.endsWith("/")) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }

    private static String errorJson(String code, String message) {
        return "{\"error\":{\"code\":\"" + code + "\",\"message\":\"" + message + "\"}}";
    }
}
