package com.youbi.monitor.controller;

import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.springframework.http.HttpStatus.CONFLICT;

@RestController
@CrossOrigin
public class SubmitterAuthorTypeController {
    private final MonitorService monitorService;

    public SubmitterAuthorTypeController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    @GetMapping("/api/submitter-author-types")
    public MonitorService.SubmitterAuthorType submitterAuthorType(@RequestParam String author) {
        return monitorService.authorType(author);
    }

    @GetMapping("/api/submitter-author-types/all")
    public List<MonitorService.SubmitterAuthorType> submitterAuthorTypes() {
        return monitorService.authorTypes();
    }

    @PostMapping("/api/submitter-author-types")
    public MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            @RequestBody MonitorService.SubmitterAuthorTypeUpdateRequest request
    ) {
        try {
            return monitorService.saveAuthorType(
                    request == null ? null : request.author(),
                    request == null ? null : request.type(),
                    request == null ? null : request.taskType(),
                    request == null ? null : request.hasBackgroundAudio(),
                    request == null ? null : request.sourceLanguage(),
                    request == null ? null : request.targetLanguage(),
                    request == null ? null : request.resetCover(),
                    request == null ? null : request.coverOrientation(),
                    request == null ? null : request.fetchNewVideos()
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @DeleteMapping("/api/submitter-author-types")
    public MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(@RequestParam String author) {
        try {
            return monitorService.deleteAuthorType(author);
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }
}
