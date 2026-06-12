package com.youbi.monitor.repository;

import com.youbi.monitor.service.MonitorService;

import java.util.List;

public interface ITaskLifecycleRepositoryService {
    boolean markTaskReady(String taskId);

    String findTaskStatus(String taskId);

    boolean hasRunningStage(String taskId);

    MonitorService.TaskStopResult stopTask(String taskId);

    void resetTaskRowsForDownloader(String taskId);

    int deleteTaskRows(String taskId);

    MonitorService.DownloaderFailureList listDownloaderFailures();

    MonitorService.DownloaderRollbackDatabaseResult rollbackDownloaderFailures(List<Long> submissionIds);
}
