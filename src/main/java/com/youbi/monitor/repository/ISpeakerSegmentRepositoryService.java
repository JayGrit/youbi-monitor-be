package com.youbi.monitor.repository;

import com.youbi.monitor.service.MonitorService;

public interface ISpeakerSegmentRepositoryService {
    MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText);
}
