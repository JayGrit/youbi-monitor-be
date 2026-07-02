package com.youbi.monitor.dto;

public record StaticAssetCreateRequest(
        String taskId,
        String type,
        String content,
        String remark
) {
}
