package com.youbi.monitor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

@RestController
public class BiliupController {
    private final BiliupService biliupService;

    public BiliupController(BiliupService biliupService) {
        this.biliupService = biliupService;
    }

    @GetMapping("/api/biliup/status")
    public BiliupStatus status() throws IOException {
        return biliupService.status();
    }

    @PostMapping("/api/biliup/login")
    public BiliupJobSnapshot login() {
        return biliupService.startLogin();
    }

    @PostMapping("/api/biliup/renew")
    public BiliupJobSnapshot renew() {
        return biliupService.startRenew();
    }

    @GetMapping("/api/biliup/jobs/{id}")
    public ResponseEntity<?> job(@PathVariable String id) {
        return biliupService.getJob(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("message", "Biliup job not found.")));
    }

    @PostMapping("/api/biliup/jobs/{id}/input")
    public ResponseEntity<?> jobInput(@PathVariable String id, @RequestBody BiliupInputRequest request) throws IOException {
        return biliupService.sendInput(id, request.input())
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("message", "Biliup job not found.")));
    }

    @PostMapping("/api/biliup/jobs/{id}/cancel")
    public ResponseEntity<?> jobCancel(@PathVariable String id) {
        return biliupService.cancelJob(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("message", "Biliup job not found.")));
    }

    @GetMapping("/api/biliup/videos")
    public BiliupCommandResult videos(
            @RequestParam(defaultValue = "all") String type,
            @RequestParam(defaultValue = "1") int fromPage,
            @RequestParam(defaultValue = "1") int maxPages
    ) throws IOException, InterruptedException {
        return biliupService.listVideos(new BiliupVideoQuery(type, fromPage, maxPages));
    }
}
