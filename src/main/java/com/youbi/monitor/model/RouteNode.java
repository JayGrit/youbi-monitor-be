package com.youbi.monitor.model;

public record RouteNode(
        String id,
        String stage,
        String subStage,
        String label,
        int order,
        String tableName
) {
}
