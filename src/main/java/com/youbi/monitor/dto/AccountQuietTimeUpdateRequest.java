package com.youbi.monitor.dto;

import java.time.LocalTime;

public record AccountQuietTimeUpdateRequest(
        LocalTime startTime,
        LocalTime endTime
) {
}
