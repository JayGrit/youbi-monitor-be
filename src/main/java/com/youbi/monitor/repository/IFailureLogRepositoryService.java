package com.youbi.monitor.repository;

import com.youbi.monitor.model.FailureLogItem;

import java.util.List;

public interface IFailureLogRepositoryService {
    List<FailureLogItem> listFailureLogs();
}
