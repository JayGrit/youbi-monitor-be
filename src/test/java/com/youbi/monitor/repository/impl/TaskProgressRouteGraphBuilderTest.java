package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.RouteEdge;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskProgressRouteGraphBuilderTest {

    @Test
    void buildsLegacyRouteGraphWhenTaskTypeIsUnknown() {
        MonitorRepository repository = mock(MonitorRepository.class);
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());
        TaskProgressRouteGraphBuilder builder = new TaskProgressRouteGraphBuilder(repository);

        TaskProgressRouteGraphBuilder.RouteGraph graph = builder.build("task-1", List.of(
                stage("downloader", "下载", "success"),
                stage("whisper", "语音识别", "running"),
                stage("uploader", "上传", "pending")
        ), LocalDateTime.parse("2026-06-26T12:00:00"));

        assertThat(graph.nodes()).extracting("id")
                .containsExactly("downloader:main", "whisper:main", "uploader:main");
        assertThat(graph.nodes()).extracting("status")
                .containsExactly("success", "running", "pending");
        assertThat(graph.edges()).containsExactly(
                new RouteEdge("downloader:main", "whisper:main"),
                new RouteEdge("whisper:main", "uploader:main")
        );
    }

    private static StageNode stage(String key, String label, String status) {
        return new StageNode(key, label, status, null, null, 0, null, null, null, null,
                null, null, List.of(), List.of(), 0, false);
    }
}
