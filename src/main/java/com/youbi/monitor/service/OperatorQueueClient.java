package com.youbi.monitor.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Service
public class OperatorQueueClient {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String baseUrl;

    public OperatorQueueClient(@Value("${youbi.operator.api-base-url}") String baseUrl) {
        this.baseUrl = baseUrl.replaceFirst("/+$", "");
    }

    public ResponseEntity<String> editQueue(String action, String body) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/queue/" + action))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return ResponseEntity.status(response.statusCode()).contentType(MediaType.APPLICATION_JSON)
                    .body(response.body());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return unavailable();
        } catch (IOException exception) {
            return unavailable();
        }
    }

    private ResponseEntity<String> unavailable() {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).contentType(MediaType.APPLICATION_JSON)
                .body("{\"message\":\"无法连接 Operator，请刷新队列确认任务状态\"}");
    }
}
