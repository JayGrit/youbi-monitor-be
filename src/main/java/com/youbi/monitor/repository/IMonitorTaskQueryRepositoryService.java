package com.youbi.monitor.repository;

import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.TaskMonitorItem;
import com.youbi.monitor.model.TaskProgressDetail;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface IMonitorTaskQueryRepositoryService {
    void ensureMonitorSchema();

    List<TaskMonitorItem> listTaskMonitorItems(LocalDateTime now, int limit, int offset, String status, String topic, String stage, String taskId, String sort);

    long countTaskMonitorItems(String status, String topic, String stage, String taskId);

    TaskProgressDetail findTaskProgress(String taskId, LocalDateTime now);

    Map<String, Object> findTaskFlowRow(String table, String idColumn, String id);

    List<Map<String, Object>> listTaskFlowRows(String table, String idColumn, String id, String orderBy, int limit);

    boolean tableExists(String table);

    List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now);
}
