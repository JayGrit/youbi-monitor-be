package com.youbi.monitor.service;

import com.youbi.monitor.dto.AccountSendAvailability;
import com.youbi.monitor.dto.UploaderAccountState;
import org.springframework.stereotype.Service;

@Service
public class AccountSendAvailabilityService {
    private final UploaderAccountService uploaderAccountService;

    public AccountSendAvailabilityService(UploaderAccountService uploaderAccountService) {
        this.uploaderAccountService = uploaderAccountService;
    }

    public AccountSendAvailability availability(String platform, String accountKey, String platformAccountTable) {
        UploaderAccountState state = uploaderAccountService.state(platform, accountKey)
                .orElseGet(() -> UploaderAccountState.defaults(platform, accountKey));
        return new AccountSendAvailability(
                state.lastUploadAt(),
                state.nextUploadAllowedAt(),
                state.todayUploadCount(),
                state.cooldownWaitingCount(),
                state.uploadRunningTaskId(),
                state.uploadRunningCount()
        );
    }
}
