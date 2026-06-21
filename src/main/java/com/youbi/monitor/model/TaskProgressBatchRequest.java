package com.youbi.monitor.model;

import java.util.List;

public record TaskProgressBatchRequest(List<String> taskIds) {
}
