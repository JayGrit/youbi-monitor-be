package com.youbi.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class BilibiliUploadController {
    private static final Logger log = LoggerFactory.getLogger(BilibiliUploadController.class);

    private final BilibiliUploadService uploadService;
    private final BilibiliPlaywrightUploadService playwrightUploadService;

    public BilibiliUploadController(BilibiliUploadService uploadService, BilibiliPlaywrightUploadService playwrightUploadService) {
        this.uploadService = uploadService;
        this.playwrightUploadService = playwrightUploadService;
    }

    @PostMapping("/api/bilibili/upload")
    public ResponseEntity<?> upload(@RequestBody BilibiliUploadRequest request) {
        try {
            BilibiliUploadResult result = uploadService.upload(request);
            if (result.success()) {
                return ResponseEntity.ok(result);
            }
            if (shouldFallbackToPlaywright(result.message())) {
                return playwrightFallbackResponse(request, result.message());
            }
            return ResponseEntity.badRequest().body(result);
        } catch (Exception exception) {
            if (shouldFallbackToPlaywright(exception.getMessage())) {
                return playwrightFallbackResponse(request, exception.getMessage());
            }
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    private boolean shouldFallbackToPlaywright(String message) {
        String text = message == null ? "" : message.toLowerCase();
        return text.contains("投稿频繁")
                || text.contains("操作频繁")
                || text.contains("频繁")
                || text.contains("风控")
                || text.contains("rate limit")
                || text.contains("too frequent");
    }

    private ResponseEntity<?> playwrightFallbackResponse(BilibiliUploadRequest request, String reason) {
        log.warn("Bilibili API upload hit frequent/rate-limit error, falling back to Playwright: {}", reason);
        try {
            BilibiliUploadResult fallback = playwrightUploadService.upload(request);
            Map<String, Object> raw = new LinkedHashMap<>();
            if (fallback.raw() != null) {
                raw.putAll(fallback.raw());
            }
            raw.put("fallbackFrom", "bilibili-api");
            raw.put("fallbackReason", reason);
            BilibiliUploadResult result = new BilibiliUploadResult(
                    fallback.success(),
                    fallback.bvid(),
                    fallback.aid(),
                    fallback.accountUid(),
                    fallback.accountName(),
                    fallback.success() ? "Bilibili API 上传触发频繁限制，已降级 Playwright 上传成功" : fallback.message(),
                    raw
            );
            return fallback.success() ? ResponseEntity.ok(result) : ResponseEntity.badRequest().body(result);
        } catch (Exception fallbackException) {
            return ResponseEntity.badRequest().body(Map.of(
                    "message", "Bilibili API 上传触发频繁限制，Playwright 降级也失败: " + fallbackException.getMessage(),
                    "fallbackFrom", "bilibili-api",
                    "fallbackReason", reason
            ));
        }
    }
}
