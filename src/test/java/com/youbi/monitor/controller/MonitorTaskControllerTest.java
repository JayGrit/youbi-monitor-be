package com.youbi.monitor.controller;

import com.youbi.monitor.model.TaskProgressBatchRequest;
import com.youbi.monitor.service.DiagnosticArtifactService;
import com.youbi.monitor.service.MonitorService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MonitorTaskControllerTest {

    @Test
    void batchProgressDeduplicatesAndLimitsCurrentPageToTwentyTasks() {
        MonitorService monitorService = mock(MonitorService.class);
        when(monitorService.getTaskProgressBatch(anyList())).thenReturn(List.of());
        MonitorTaskController controller = new MonitorTaskController(
                monitorService,
                mock(DiagnosticArtifactService.class)
        );
        List<String> requested = new ArrayList<>();
        requested.add(" task-1 ");
        requested.add("task-1");
        for (int index = 2; index <= 25; index++) requested.add("task-" + index);

        controller.progressBatch(new TaskProgressBatchRequest(requested));

        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(monitorService).getTaskProgressBatch(captor.capture());
        assertThat(captor.getValue())
                .hasSize(20)
                .doesNotHaveDuplicates()
                .startsWith("task-1", "task-2")
                .endsWith("task-20");
    }
}
