package com.youbi.monitor;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

@RestController
public class AliDriveController {
    private final AliDriveService aliDriveService;

    public AliDriveController(AliDriveService aliDriveService) {
        this.aliDriveService = aliDriveService;
    }

    @GetMapping("/api/alidrive/me")
    public Map<String, Object> me() {
        try {
            return aliDriveService.me();
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            return Map.of("success", false, "message", exc.getMessage());
        } catch (IOException exc) {
            return Map.of("success", false, "message", exc.getMessage());
        }
    }

    @GetMapping("/api/alidrive/list")
    public Map<String, Object> list(@RequestParam(defaultValue = "/") String path) {
        try {
            return aliDriveService.list(path);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            return Map.of("success", false, "message", exc.getMessage());
        } catch (IOException exc) {
            return Map.of("success", false, "message", exc.getMessage());
        }
    }

    @PostMapping("/api/alidrive/upload")
    public AliDriveTransferResult upload(@RequestBody AliDriveUploadRequest request) {
        try {
            return aliDriveService.upload(request);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            return new AliDriveTransferResult(false, exc.getMessage(), "", "", "", "", 0, Map.of());
        } catch (IOException exc) {
            return new AliDriveTransferResult(false, exc.getMessage(), "", "", "", "", 0, Map.of());
        }
    }

    @PostMapping("/api/alidrive/download")
    public AliDriveTransferResult download(@RequestBody AliDriveDownloadRequest request) {
        try {
            return aliDriveService.download(request);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            return new AliDriveTransferResult(false, exc.getMessage(), "", "", "", "", 0, Map.of());
        } catch (IOException exc) {
            return new AliDriveTransferResult(false, exc.getMessage(), "", "", "", "", 0, Map.of());
        }
    }
}
