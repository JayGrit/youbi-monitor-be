package com.youbi.monitor;

import java.util.List;

public record WhisperProcessingDetail(
        List<RawSegment> rawSegments,
        List<AlignedSegment> alignedSegments,
        List<PysbdSegment> pysbdSegments,
        List<SplitSegment> splitSegments
) {
    public record RawSegment(
            long id,
            int rawIndex,
            String text,
            int startTime,
            int endTime
    ) {
    }

    public record AlignedSegment(
            long id,
            Long rawSegmentId,
            int alignedIndex,
            String text,
            int startTime,
            int endTime
    ) {
    }

    public record PysbdSegment(
            long id,
            int pysbdIndex,
            String text,
            int startTime,
            int endTime
    ) {
    }

    public record SplitSegment(
            long id,
            int splitIndex,
            long pysbdSegmentId,
            String text,
            int startTime,
            int endTime,
            String splitReason,
            String splitMethod,
            String splitPunctuation,
            String splitConjunction
    ) {
    }
}
