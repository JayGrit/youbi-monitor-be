package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.RouteEdge;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

    @Test
    void usesTaskRouteSnapshotWhenPresent() throws Exception {
        MonitorRepository repository = mock(MonitorRepository.class);
        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class)))
                .thenAnswer(TaskProgressRouteGraphBuilderTest::tableExists);
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenAnswer(TaskProgressRouteGraphBuilderTest::snapshotRows);
        TaskProgressRouteGraphBuilder builder = new TaskProgressRouteGraphBuilder(repository);

        TaskProgressRouteGraphBuilder.RouteGraph graph = builder.build("FQ8xTt6fCPY", List.of(
                stage("downloader", "下载", "success"),
                stage("demucs", "人声分离", "skipped"),
                stage("translator", "翻译", "skipped")
        ), LocalDateTime.parse("2026-07-05T12:00:00"));

        assertThat(graph.nodes()).extracting("id")
                .containsExactly("downloader:metadata", "publisher:script_generation");
        assertThat(graph.edges()).containsExactly(
                new RouteEdge("downloader:metadata", "publisher:script_generation")
        );
    }

    private static Integer tableExists(InvocationOnMock invocation) {
        Object[] args = Arrays.copyOfRange(invocation.getArguments(), 2, invocation.getArguments().length);
        String table = String.valueOf(args[0]);
        return List.of(
                "distributor_task_route_nodes",
                "distributor_task_route_edges",
                "distributor_task_stages",
                "distributor_route_log"
        ).contains(table) ? 1 : 0;
    }

    @SuppressWarnings("unchecked")
    private static Object snapshotRows(InvocationOnMock invocation) throws Exception {
        String sql = invocation.getArgument(0);
        RowMapper<Object> mapper = invocation.getArgument(1);
        List<Object> result = new ArrayList<>();
        if (sql.contains("FROM distributor_task_route_nodes")) {
            result.add(mapper.mapRow(routeNode("downloader", "metadata", 1), 0));
            result.add(mapper.mapRow(routeNode("publisher", "script_generation", 2), 1));
        } else if (sql.contains("FROM distributor_task_route_edges")) {
            result.add(mapper.mapRow(routeEdge("publisher", "script_generation", "downloader", "metadata"), 0));
        }
        return result;
    }

    private static ResultSet routeNode(String stage, String subStage, int order) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("stage_name")).thenReturn(stage);
        when(rs.getString("sub_stage")).thenReturn(subStage);
        when(rs.getInt("stage_order")).thenReturn(order);
        return rs;
    }

    private static ResultSet routeEdge(String stage, String subStage, String parentStage, String parentSubStage) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("stage_name")).thenReturn(stage);
        when(rs.getString("sub_stage")).thenReturn(subStage);
        when(rs.getString("depends_on_stage_name")).thenReturn(parentStage);
        when(rs.getString("depends_on_sub_stage")).thenReturn(parentSubStage);
        return rs;
    }

    private static StageNode stage(String key, String label, String status) {
        return new StageNode(key, label, status, null, null, 0, null, null, null, null,
                null, null, List.of(), List.of(), 0, false);
    }
}
