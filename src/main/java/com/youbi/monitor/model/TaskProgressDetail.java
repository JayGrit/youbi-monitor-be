package com.youbi.monitor.model;

import java.util.List;

public record TaskProgressDetail(
        List<String> distributorStages,
        List<StageNode> nodes
) {
}
