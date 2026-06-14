package com.youbi.monitor.repository;

import com.youbi.monitor.model.FailureLogItem;
import com.youbi.monitor.model.FailureLogActualPublishedResult;

import java.util.List;

public interface IFailureLogRepositoryService {
    List<FailureLogItem> listFailureLogs();

    FailureLogActualPublishedResult markActualPublished(String logId);
}
