package com.youbi.monitor.model;

public record UploaderPhoneAccountOption(
        Long id,
        String topic,
        String displayName,
        String remark,
        String avatarUrl,
        Boolean isAvailable
) {
}
