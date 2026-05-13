package com.youbi.monitor;

import java.time.LocalDateTime;

public record DeviceHeartbeat(String deviceName, LocalDateTime lastSeenAt, boolean online, long secondsSinceLastSeen) {
}
