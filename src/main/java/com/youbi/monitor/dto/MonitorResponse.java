package com.youbi.monitor.dto;

import com.youbi.monitor.model.TaskMonitorItem;
import java.time.LocalDateTime;
import java.util.List;

public record MonitorResponse(
        List<TaskMonitorItem> tasks,
        List<ServiceHeartbeat> serviceHeartbeats,
        LocalDateTime serverTime,
        int page,
        int pageSize,
        long totalCount
) {
}
