package com.youbi.monitor;

public record DouyinCdpSessionUpdateRequest(
        String originalAccountKey,
        String accountKey,
        Integer cdpPort
) {
}
