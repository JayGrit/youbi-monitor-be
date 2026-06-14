package com.youbi.monitor.controller;

import com.youbi.monitor.dto.FailureLogResponse;
import com.youbi.monitor.model.FailureLogActualPublishedResult;
import com.youbi.monitor.repository.IFailureLogRepositoryService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;

import static org.springframework.http.HttpStatus.CONFLICT;

@RestController
@CrossOrigin
public class FailureLogController {
    private final IFailureLogRepositoryService failureLogRepositoryService;

    public FailureLogController(IFailureLogRepositoryService failureLogRepositoryService) {
        this.failureLogRepositoryService = failureLogRepositoryService;
    }

    @GetMapping("/api/failure-logs")
    public FailureLogResponse failureLogs() {
        var rows = failureLogRepositoryService.listFailureLogs();
        return new FailureLogResponse(rows.size(), rows, LocalDateTime.now());
    }

    @PostMapping("/api/failure-logs/{logId}/actual-published")
    public FailureLogActualPublishedResult markActualPublished(@PathVariable String logId) {
        try {
            return failureLogRepositoryService.markActualPublished(logId);
        } catch (IllegalArgumentException | IllegalStateException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }
}
