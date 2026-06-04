package com.youbi.monitor.dto;

import java.util.List;

public record ServiceHeartbeat(String serviceName, String label, List<DeviceHeartbeat> devices) {
}
