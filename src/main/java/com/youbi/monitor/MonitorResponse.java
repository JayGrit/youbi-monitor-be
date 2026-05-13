package com.youbi.monitor;

import java.time.LocalDateTime;
import java.util.List;

public record MonitorResponse(
        List<TaskMonitorItem> tasks,
        List<ServiceHeartbeat> serviceHeartbeats,
        LocalDateTime serverTime
) {
}
