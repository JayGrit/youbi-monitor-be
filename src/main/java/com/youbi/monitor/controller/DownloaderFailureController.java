package com.youbi.monitor.controller;

import com.youbi.monitor.service.MonitorService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.List;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@RestController
@CrossOrigin
public class DownloaderFailureController {
    private final MonitorService monitorService;

    public DownloaderFailureController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    @GetMapping("/api/downloader-failures")
    public MonitorService.DownloaderFailureList downloaderFailures() {
        return monitorService.downloaderFailures();
    }

    @PostMapping("/api/downloader-failures/rollback")
    public MonitorService.DownloaderRollbackResult rollbackDownloaderFailures(
            @RequestBody MonitorService.DownloaderRollbackRequest request
    ) {
        try {
            return monitorService.rollbackDownloaderFailures(
                    request == null ? List.of() : request.submissionIds()
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        } catch (IOException exc) {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, exc.getMessage(), exc);
        }
    }
}
