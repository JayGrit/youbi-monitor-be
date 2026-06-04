package com.youbi.monitor.repository;

import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.TaskMonitorItem;
import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperWordTimestamp;
import com.youbi.monitor.service.MonitorService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface IMonitorRepositoryService {
    void ensureMonitorSchema();

    List<TaskMonitorItem> listTaskMonitorItems(LocalDateTime now, int limit);

    Map<String, Object> findTaskFlowRow(String table, String idColumn, String id);

    List<Map<String, Object>> listTaskFlowRows(String table, String idColumn, String id, String orderBy, int limit);

    boolean tableExists(String table);

    List<WhisperWordTimestamp> listWhisperWordTimestamps(String taskId);

    WhisperProcessingDetail findWhisperProcessing(String taskId);

    MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText);

    MonitorService.FailedUploadSubmissionList listFailedUploadSubmissions(String platform);

    MonitorService.UploadSubmissionRetryResult retryFailedUploadSubmissions(String platform, List<Long> ids);

    MonitorService.UploadBackfillCandidateList listUploadBackfillCandidates(String platform, String accountKey, String type);

    MonitorService.UploadBackfillRegisterResult registerUploadBackfill(String platform, String accountKey, String type, List<String> taskIds);

    MonitorService.SubmitterAuthorType findSubmitterAuthorType(String author);

    List<MonitorService.SubmitterAuthorType> listSubmitterAuthorTypes();

    MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            String author,
            String type,
            Boolean needSubtitle,
            Boolean needDubbing,
            Boolean needSeparation,
            String sourceLanguage,
            String targetLanguage
    );

    MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(String author);

    boolean markTaskReady(String taskId);

    String findTaskStatus(String taskId);

    boolean hasRunningStage(String taskId);

    MonitorService.TaskStopResult stopTask(String taskId);

    void resetTaskRowsForDownloader(String taskId);

    int deleteTaskRows(String taskId);

    List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now);
}
