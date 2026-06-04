package com.youbi.monitor.repository;

import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.TaskMonitorItem;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface IMonitorTaskQueryRepositoryService {
    void ensureMonitorSchema();

    List<TaskMonitorItem> listTaskMonitorItems(LocalDateTime now, int limit, int offset);

    long countTaskMonitorItems();

    Map<String, Object> findTaskFlowRow(String table, String idColumn, String id);

    List<Map<String, Object>> listTaskFlowRows(String table, String idColumn, String id, String orderBy, int limit);

    boolean tableExists(String table);

    List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now);
}
