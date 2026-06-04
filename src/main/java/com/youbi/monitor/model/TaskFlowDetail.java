package com.youbi.monitor.model;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public record TaskFlowDetail(
        Map<String, Object> task,
        Map<String, Object> videoInfo,
        List<TaskFlowStage> stages,
        List<TaskFlowAsset> minioObjects,
        LocalDateTime serverTime
) {
    public record TaskFlowStage(
            String key,
            String label,
            String status,
            LocalDateTime startedAt,
            LocalDateTime completedAt,
            long elapsedSeconds,
            String operator,
            String errorMessage,
            List<TaskFlowField> inputs,
            List<TaskFlowField> outputs,
            List<TaskFlowTable> tables
    ) {
    }

    public record TaskFlowField(
            String name,
            Object value,
            TaskFlowAsset asset
    ) {
    }

    public record TaskFlowTable(
            String name,
            List<Map<String, Object>> rows,
            boolean truncated
    ) {
    }

    public record TaskFlowAsset(
            String name,
            String stage,
            String kind,
            String url,
            String objectName,
            Long size,
            LocalDateTime lastModified
    ) {
    }
}
