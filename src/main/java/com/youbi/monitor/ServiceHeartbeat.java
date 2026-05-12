package com.youbi.monitor;

import java.util.List;

public record ServiceHeartbeat(String serviceName, String label, List<DeviceHeartbeat> devices) {
}
