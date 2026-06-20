package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.RouteNode;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.service.StageRegistry;
import com.youbi.monitor.service.TaskRouteService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskLifecycleRepositoryServiceImplTest {

    @Test
    void runningGuardIncludesAsseterStage() {
        MonitorRepository repository = mock(MonitorRepository.class);
        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.contains("INFORMATION_SCHEMA.TABLES")) return 1;
            return sql.contains("FROM `asseter`") ? 1 : 0;
        });
        TaskLifecycleRepositoryServiceImpl service = new TaskLifecycleRepositoryServiceImpl(
                repository, mock(TaskRouteService.class), new StageRegistry());

        assertThat(service.hasRunningStage("task-1")).isTrue();
    }

    @Test
    void stopUsesEffectiveRouteAndStopsAsseterJobs() {
        MonitorRepository repository = mock(MonitorRepository.class);
        TaskRouteService routes = mock(TaskRouteService.class);
        when(repository.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(List.of("running"));
        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenReturn(1);
        when(routes.routeForTask("task-1")).thenReturn(List.of(
                new RouteNode("asseter:main", "asseter", "main", "素材加工", 1, "asseter")
        ));
        when(repository.update(anyString(), any(Object[].class))).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            return sql.contains("UPDATE `asseter`") || sql.contains("UPDATE asseter_jobs") ? 1 : 0;
        });
        TaskLifecycleRepositoryServiceImpl service = new TaskLifecycleRepositoryServiceImpl(
                repository, routes, new StageRegistry());

        var result = service.stopTask("task-1");

        assertThat(result.status()).isEqualTo("failed");
        assertThat(result.stoppedStages()).isEqualTo(2);
        assertThat(result.stoppedTask()).isTrue();
    }
}
