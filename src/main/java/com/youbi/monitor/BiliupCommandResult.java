package com.youbi.monitor;

public record BiliupCommandResult(
        String command,
        int exitCode,
        String output
) {
}
