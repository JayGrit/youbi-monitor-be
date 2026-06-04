package com.youbi.monitor.repository;

import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperWordTimestamp;

import java.util.List;

public interface IWhisperProcessingRepositoryService {
    List<WhisperWordTimestamp> listWhisperWordTimestamps(String taskId);

    WhisperProcessingDetail findWhisperProcessing(String taskId);
}
