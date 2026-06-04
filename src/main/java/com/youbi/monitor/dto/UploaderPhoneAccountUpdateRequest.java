package com.youbi.monitor.dto;

public record UploaderPhoneAccountUpdateRequest(
        Long accountId,
        String note,
        Boolean disabled
) {
}
