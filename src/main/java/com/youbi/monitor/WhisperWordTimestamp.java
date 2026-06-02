package com.youbi.monitor;

public record WhisperWordTimestamp(
        String taskId,
        String segmentType,
        int segmentIndex,
        int wordIndex,
        String text,
        int startTime,
        int endTime
) {
}
