package com.youbi.monitor.controller;

import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
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
public class UploadSubmissionMonitorController {
    private final MonitorService monitorService;

    public UploadSubmissionMonitorController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    @GetMapping("/api/upload-backfill/candidates")
    public MonitorService.UploadBackfillCandidateList uploadBackfillCandidates(
            @RequestParam String platform,
            @RequestParam String accountKey,
            @RequestParam String type
    ) {
        try {
            return monitorService.uploadBackfillCandidates(platform, accountKey, type);
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/upload-backfill/register")
    public MonitorService.UploadBackfillRegisterResult registerUploadBackfill(
            @RequestBody MonitorService.UploadBackfillRegisterRequest request
    ) {
        try {
            return monitorService.registerUploadBackfill(
                    request == null ? null : request.platform(),
                    request == null ? null : request.accountKey(),
                    request == null ? null : request.type(),
                    request == null ? List.of() : request.taskIds()
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }
}
