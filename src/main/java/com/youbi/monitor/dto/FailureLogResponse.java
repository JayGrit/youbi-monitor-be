package com.youbi.monitor.dto;

import com.youbi.monitor.model.FailureLogItem;

import java.time.LocalDateTime;
import java.util.List;

public record FailureLogResponse(
        int count,
        List<FailureLogItem> rows,
        LocalDateTime loadedAt
) {
}
