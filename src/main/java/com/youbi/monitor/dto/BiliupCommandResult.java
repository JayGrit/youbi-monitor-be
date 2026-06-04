package com.youbi.monitor.dto;

public record BiliupCommandResult(
        String command,
        int exitCode,
        String output
) {
}
