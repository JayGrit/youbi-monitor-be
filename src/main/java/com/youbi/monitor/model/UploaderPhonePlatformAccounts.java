package com.youbi.monitor.model;

import java.util.List;

public record UploaderPhonePlatformAccounts(
        String platform,
        List<UploaderPhoneAccountOption> accounts
) {
}
