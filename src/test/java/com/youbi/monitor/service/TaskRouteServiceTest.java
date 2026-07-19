package com.youbi.monitor.service;

import com.youbi.monitor.model.RouteNode;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskRouteServiceTest {

    @Test
    void readsAllFiveDistributorRoutesAndPreservesCombinerIdentity() throws Exception {
        assertRoute("repost", true, rows("downloader:main", "publisher:main", "uploader:main"));
        assertRoute("subtitle", true, rows("downloader:main", "publisher:main", "demucs:main", "whisper:main", "translator:main", "combiner:main", "uploader:main"));
        assertRoute("dubbing", true, rows("downloader:main", "publisher:main", "demucs:main", "whisper:main", "translator:main", "speaker:main", "combiner:main", "uploader:main"));
        List<RouteNode> narration = assertRoute("narration", true, rows(
                "downloader:metadata", "downloader:audio", "demucs:main", "whisper:source_transcription",
                "publisher:script_generation", "publisher:publish_metadata", "publisher:segment_plan",
                "publisher:image_generation", "asseter:image_composition", "speaker:narration",
                "combiner:audio_merge", "whisper:main", "asseter:audio_visualization",
                "combiner:video_render", "uploader:main"));
        assertThat(narration).extracting(RouteNode::id).contains("combiner:audio_merge", "combiner:video_render");
        assertThat(narration).extracting(RouteNode::label).contains("音频合并", "视频渲染");
        List<RouteNode> asmr = assertRoute("asmr", true, rows("downloader:main", "publisher:main", "combiner:asmr"));
        assertThat(asmr.get(2).label()).isEqualTo("ASMR 合成");
    }

    @Test
    void removesDemucsWhenBackgroundAudioIsDisabled() throws Exception {
        List<RouteNode> route = assertRoute("subtitle", false, rows("downloader:main", "publisher:main", "demucs:main", "whisper:main", "translator:main", "combiner:main", "uploader:main"));
        assertThat(route).extracting(RouteNode::stage).doesNotContain("demucs");
    }

    @Test
    void unknownConfiguredStageFailsClosed() throws Exception {
        TaskRouteService service = service("dubbing", true, rows("downloader:main", "future-stage:main"));
        assertThatThrownBy(() -> service.routeForTask("task-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not registered");
    }

    private static List<RouteNode> assertRoute(String taskType, boolean background, List<String[]> configured) throws Exception {
        TaskRouteService service = service(taskType, background, configured);
        List<RouteNode> route = service.routeForTask("task-1");
        assertThat(route).extracting(RouteNode::id)
                .containsExactly(configured.stream()
                        .filter(row -> background || !"demucs".equals(row[0]))
                        .map(row -> row[0] + ":" + row[1])
                        .toArray(String[]::new));
        return route;
    }

    private static TaskRouteService service(String taskType, boolean background, List<String[]> configured) throws Exception {
        MonitorRepository repository = mock(MonitorRepository.class);
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenAnswer(invocation -> rows(invocation, taskType, background, configured));
        return new TaskRouteService(repository, new StageRegistry());
    }

    @SuppressWarnings("unchecked")
    private static Object rows(InvocationOnMock invocation, String taskType, boolean background, List<String[]> configured) throws Exception {
        String sql = invocation.getArgument(0);
        RowMapper<Object> mapper = invocation.getArgument(1);
        List<Object> result = new ArrayList<>();
        if (sql.contains("FROM task t")) {
            ResultSet rs = mock(ResultSet.class);
            when(rs.getString("task_type")).thenReturn(taskType);
            when(rs.getObject("has_background_audio")).thenReturn(background);
            when(rs.getBoolean("has_background_audio")).thenReturn(background);
            when(rs.getObject("has_native_subtitle")).thenReturn(Boolean.FALSE);
            when(rs.getBoolean("has_native_subtitle")).thenReturn(false);
            result.add(mapper.mapRow(rs, 0));
        } else if (sql.contains("FROM distributor_type_stages")) {
            int index = 0;
            for (String[] row : configured) {
                ResultSet rs = mock(ResultSet.class);
                when(rs.getString("stage_name")).thenReturn(row[0]);
                when(rs.getString("sub_stage")).thenReturn(row[1]);
                result.add(mapper.mapRow(rs, index++));
            }
        }
        return result;
    }

    private static List<String[]> rows(String... values) {
        List<String[]> rows = new ArrayList<>();
        for (String value : values) rows.add(value.split(":"));
        return rows;
    }
}
