package com.youbi.monitor.controller;

import com.youbi.monitor.dto.NarrationSegmentManualRequest;
import com.youbi.monitor.service.NarrationManualService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@RestController
@CrossOrigin
public class NarrationManualController {
    private final NarrationManualService narrationManualService;

    public NarrationManualController(NarrationManualService narrationManualService) {
        this.narrationManualService = narrationManualService;
    }

    // 手动提交指定任务的口播分段内容。
    @PostMapping("/api/video-tasks/{taskId}/publisher/narration/segments")
    public ResponseEntity<?> submitSegments(
            @PathVariable String taskId,
            @RequestBody(required = false) NarrationSegmentManualRequest request
    ) {
        try {
            return ResponseEntity.ok(narrationManualService.submitSegments(
                    taskId,
                    request == null ? "" : request.response()
            ));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }

    // 上传指定任务口播发布所需的图片。
    @PostMapping(
            value = "/api/video-tasks/{taskId}/publisher/narration/images/{kind}",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE
    )
    public ResponseEntity<?> uploadImage(
            @PathVariable String taskId,
            @PathVariable String kind,
            @RequestParam("file") MultipartFile file
    ) {
        try {
            return ResponseEntity.ok(narrationManualService.uploadImage(taskId, kind, file));
        } catch (Exception exception) {
            return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
        }
    }
}
