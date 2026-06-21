package com.youbi.monitor.model;

import java.util.List;

public record TaskProgressDetail(
        String taskId,
        List<String> distributorStages,
        List<StageNode> nodes,
        List<TaskProgressRouteNode> routeNodes,
        List<RouteEdge> routeEdges
) {
}
