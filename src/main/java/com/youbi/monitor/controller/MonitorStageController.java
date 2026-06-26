package com.youbi.monitor.controller;

import com.youbi.monitor.dto.SpeakerSegmentTextUpdateRequest;
import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperWordTimestamp;
import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@CrossOrigin
public class MonitorStageController {
    private final MonitorService monitorService;

    public MonitorStageController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    @GetMapping("/api/video-tasks/{taskId}/whisper-word-timestamps")
    public List<WhisperWordTimestamp> whisperWordTimestamps(@PathVariable String taskId) {
        return monitorService.whisperWordTimestamps(taskId);
    }

    @GetMapping("/api/video-tasks/{taskId}/whisper-processing")
    public WhisperProcessingDetail whisperProcessing(@PathVariable String taskId) {
        return monitorService.whisperProcessing(taskId);
    }

    @PatchMapping("/api/video-tasks/{taskId}/speaker-segments/{segmentId}/dst-text")
    public MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(
            @PathVariable String taskId,
            @PathVariable long segmentId,
            @RequestBody SpeakerSegmentTextUpdateRequest request
    ) {
        MonitorService.SpeakerSegmentTextUpdateResult result = monitorService.updateSpeakerSegmentDstText(
                taskId,
                segmentId,
                request == null ? null : request.dstText()
        );
        if (result == null) {
            throw new ResponseStatusException(NOT_FOUND, "Speaker segment does not exist.");
        }
        return result;
    }

    @PatchMapping("/api/video-tasks/{taskId}/translator-segments/{itemIndex}/dst-text")
    public MonitorService.TranslatorSegmentTextUpdateResult updateTranslatorSegmentDstText(
            @PathVariable String taskId,
            @PathVariable int itemIndex,
            @RequestBody SpeakerSegmentTextUpdateRequest request
    ) {
        MonitorService.TranslatorSegmentTextUpdateResult result = monitorService.updateTranslatorSegmentDstText(
                taskId,
                itemIndex,
                request == null ? null : request.dstText()
        );
        if (result == null) {
            throw new ResponseStatusException(NOT_FOUND, "Translator segment does not exist.");
        }
        return result;
    }
}
