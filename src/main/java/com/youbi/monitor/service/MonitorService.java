package com.youbi.monitor.service;

import com.youbi.monitor.dto.MonitorResponse;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.TaskFlowDetail;
import com.youbi.monitor.model.TaskMonitorItem;
import com.youbi.monitor.model.TaskProgressDetail;
import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperWordTimestamp;
import com.youbi.monitor.repository.IMonitorTaskQueryRepositoryService;
import com.youbi.monitor.repository.ISpeakerSegmentRepositoryService;
import com.youbi.monitor.repository.IWhisperProcessingRepositoryService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class MonitorService {
    private final IMonitorTaskQueryRepositoryService taskQueryRepositoryService;
    private final IWhisperProcessingRepositoryService whisperProcessingRepositoryService;
    private final ISpeakerSegmentRepositoryService speakerSegmentRepositoryService;
    private final TaskFlowService taskFlowService;

    public MonitorService(
            IMonitorTaskQueryRepositoryService taskQueryRepositoryService,
            IWhisperProcessingRepositoryService whisperProcessingRepositoryService,
            ISpeakerSegmentRepositoryService speakerSegmentRepositoryService,
            TaskFlowService taskFlowService
    ) {
        this.taskQueryRepositoryService = taskQueryRepositoryService;
        this.whisperProcessingRepositoryService = whisperProcessingRepositoryService;
        this.speakerSegmentRepositoryService = speakerSegmentRepositoryService;
        this.taskFlowService = taskFlowService;
        this.taskQueryRepositoryService.ensureMonitorSchema();
    }

    public MonitorResponse listTasks(int page, int limit, String status, String type, String stage, String taskId, String sort) {
        LocalDateTime now = LocalDateTime.now();
        int offset = Math.max(0, page - 1) * limit;
        List<TaskMonitorItem> tasks = taskQueryRepositoryService.listTaskMonitorItems(now, limit, offset, status, type, stage, taskId, sort);
        long totalCount = taskQueryRepositoryService.countTaskMonitorItems(status, type, stage, taskId);
        return new MonitorResponse(tasks, now, page, limit, totalCount);
    }

    public List<ServiceHeartbeat> listServiceHeartbeats() {
        return taskQueryRepositoryService.listServiceHeartbeats(LocalDateTime.now());
    }

    public TaskFlowDetail getTaskFlow(String taskId, String requestedStage) {
        return taskFlowService.getTaskFlow(taskId, requestedStage);
    }

    public TaskProgressDetail getTaskProgress(String taskId) {
        return taskQueryRepositoryService.findTaskProgress(taskId, LocalDateTime.now());
    }

    public List<TaskProgressDetail> getTaskProgressBatch(List<String> taskIds) {
        LocalDateTime now = LocalDateTime.now();
        return taskIds.stream()
                .map(taskId -> taskQueryRepositoryService.findTaskProgress(taskId, now))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    public List<WhisperWordTimestamp> whisperWordTimestamps(String taskId) {
        return whisperProcessingRepositoryService.listWhisperWordTimestamps(taskId);
    }

    public WhisperProcessingDetail whisperProcessing(String taskId) {
        return whisperProcessingRepositoryService.findWhisperProcessing(taskId);
    }

    public SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText) {
        return speakerSegmentRepositoryService.updateSpeakerSegmentDstText(taskId, segmentId, dstText);
    }

    public TranslatorSegmentTextUpdateResult updateTranslatorSegmentDstText(String taskId, int itemIndex, String dstText) {
        return speakerSegmentRepositoryService.updateTranslatorSegmentDstText(taskId, itemIndex, dstText);
    }

    public record TaskRestartResult(String status, int deletedMinioObjects) {
    }

    public record TaskStopResult(String status, int stoppedStages, boolean stoppedTask) {
    }

    public record TaskDeleteResult(String status, int deletedDatabaseRows, int deletedMinioObjects) {
    }

    public record SpeakerSegmentTextUpdateResult(
            long id,
            String taskId,
            Integer itemIndex,
            String dstText,
            LocalDateTime updatedAt
    ) {
    }

    public record TranslatorSegmentTextUpdateResult(
            String taskId,
            int itemIndex,
            String dstText,
            LocalDateTime updatedAt
    ) {
    }

    public record FailedUploadSubmission(
            long id,
            String platform,
            String taskId,
            String title,
            String accountKey,
            String errorMessage,
            LocalDateTime completedAt,
            LocalDateTime updatedAt,
            String taskStatus,
            String uploaderStatus,
            String uploadPlatforms,
            String routedAccountKey,
            boolean accountExists,
            String retryBlockedReason
    ) {
    }

    public record FailedUploadSubmissionList(String platform, int count, List<FailedUploadSubmission> rows) {
    }

    public record DownloaderFailure(
            long submissionId,
            String taskId,
            String title,
            String type,
            String errorMessage,
            LocalDateTime completedAt,
            String sourceUrl
    ) {
    }

    public record DownloaderFailureList(int count, List<DownloaderFailure> rows) {
    }

    public record DownloaderRollbackRequest(List<Long> submissionIds) {
    }

    public record DownloaderRollbackDatabaseResult(int restoredSubmissionCount, int deletedDatabaseRows) {
    }

    public record DownloaderRollbackResult(
            int restoredSubmissionCount,
            int deletedDatabaseRows,
            int deletedMinioObjects
    ) {
    }
}
