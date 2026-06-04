package com.youbi.monitor.model;

import java.util.List;

public record UploaderPhoneMatrixResponse(
        List<UploaderPhoneRecord> phones,
        List<UploaderPhonePlatformAccounts> platforms
) {
}
