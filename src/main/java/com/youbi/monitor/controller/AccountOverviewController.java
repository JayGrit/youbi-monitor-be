package com.youbi.monitor.controller;

import com.youbi.monitor.service.AccountOverviewService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class AccountOverviewController {
    private final AccountOverviewService accountOverviewService;

    public AccountOverviewController(AccountOverviewService accountOverviewService) {
        this.accountOverviewService = accountOverviewService;
    }

    @GetMapping("/api/accounts/overview")
    public Map<String, List<Map<String, Object>>> overview() {
        return accountOverviewService.overview();
    }
}
