package com.youbi.monitor.controller;

import com.youbi.monitor.dto.FailureLogResponse;
import com.youbi.monitor.repository.IFailureLogRepositoryService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

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
}
