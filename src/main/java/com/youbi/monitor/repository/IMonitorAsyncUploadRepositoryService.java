package com.youbi.monitor.repository;

import com.youbi.monitor.model.MonitorUploadTaskRow;

import java.util.Optional;

public interface IMonitorAsyncUploadRepositoryService {
    void ensureSchema();

    long countActiveTasks();

    void insertAcceptedTask(String uploadTaskId, String platform, String upstreamTaskId, String accountKey);

    Optional<MonitorUploadTaskRow> findByUploadTaskId(String uploadTaskId);

    boolean markRunning(String uploadTaskId);

    boolean markSuccess(String uploadTaskId, String resultJson);

    boolean markFailed(String uploadTaskId, String resultJson, String errorCode, String errorMessage);

    void failStaleRunningTasks(String errorMessage, int timeoutSeconds);
}
