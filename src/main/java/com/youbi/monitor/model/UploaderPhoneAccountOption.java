package com.youbi.monitor.model;

public record UploaderPhoneAccountOption(
        Long id,
        String accountKey,
        String displayName,
        String remark,
        String avatarUrl
) {
}
