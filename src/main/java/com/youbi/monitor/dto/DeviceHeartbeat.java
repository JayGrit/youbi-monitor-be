package com.youbi.monitor.dto;

import java.time.LocalDateTime;

public record DeviceHeartbeat(String deviceName, LocalDateTime lastSeenAt, boolean online, long secondsSinceLastSeen) {
}
