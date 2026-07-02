package com.youbi.monitor.controller;

import com.youbi.monitor.dto.StaticAssetCreateRequest;
import com.youbi.monitor.service.StaticAssetService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@RestController
@CrossOrigin
public class StaticAssetController {
    private final StaticAssetService staticAssetService;

    public StaticAssetController(StaticAssetService staticAssetService) {
        this.staticAssetService = staticAssetService;
    }

    @GetMapping("/api/assets")
    public ResponseEntity<?> listAssets(
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String taskId,
            @RequestParam(required = false) String scope,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset
    ) {
        try {
            return ResponseEntity.ok(staticAssetService.listAssets(type, taskId, scope, keyword, limit, offset));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @GetMapping("/api/assets/{id}")
    public ResponseEntity<?> getAsset(@PathVariable long id) {
        try {
            return ResponseEntity.ok(staticAssetService.getAsset(id));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping("/api/assets")
    public ResponseEntity<?> createTextAsset(@RequestBody(required = false) StaticAssetCreateRequest request) {
        try {
            return ResponseEntity.ok(staticAssetService.createTextAsset(request));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    @PostMapping(value = "/api/assets/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadAsset(
            @RequestParam("type") String type,
            @RequestParam(required = false) String taskId,
            @RequestParam(required = false) String remark,
            @RequestParam("file") MultipartFile file
    ) {
        try {
            return ResponseEntity.ok(staticAssetService.uploadAsset(type, taskId, remark, file));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
