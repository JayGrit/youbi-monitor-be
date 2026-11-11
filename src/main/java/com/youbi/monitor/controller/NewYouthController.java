package com.youbi.monitor.controller;

import com.youbi.monitor.service.NewYouthService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class NewYouthController {
    private final NewYouthService service;

    public NewYouthController(NewYouthService service) {
        this.service = service;
    }

    @GetMapping("/api/newyouth/overview")
    public Map<String, Object> overview() {
        return service.overview();
    }
}
