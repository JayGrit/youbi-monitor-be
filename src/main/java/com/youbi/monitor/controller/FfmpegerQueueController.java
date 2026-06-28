package com.youbi.monitor.controller;

import com.youbi.monitor.service.QueueMonitorService;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/ffmpeger")
public class FfmpegerQueueController {
    private final QueueMonitorService queueMonitorService;

    public FfmpegerQueueController(QueueMonitorService queueMonitorService) {
        this.queueMonitorService = queueMonitorService;
    }

    @GetMapping("/queue")
    public Map<String, Object> queue(@RequestParam MultiValueMap<String, String> query) {
        return queueMonitorService.listFfmpegerQueue(query);
    }
}
