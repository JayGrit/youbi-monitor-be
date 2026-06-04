package com.youbi.monitor.model;

public record WhisperWordTimestamp(
        String taskId,
        int segmentIndex,
        int wordIndex,
        String text,
        int startTime,
        int endTime
) {
}
