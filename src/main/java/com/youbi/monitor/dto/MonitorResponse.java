package com.youbi.monitor.dto;

import com.youbi.monitor.model.TaskMonitorSummary;
import java.time.LocalDateTime;
import java.util.List;

public record MonitorResponse(
        List<TaskMonitorSummary> tasks,
        LocalDateTime serverTime,
        int page,
        int pageSize,
        long totalCount
) {
}
