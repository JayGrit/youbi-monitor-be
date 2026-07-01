package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MonitorTaskQueryRepositoryServiceImplTest {

    @Test
    void summaryQueryDoesNotJoinStageOrChildTables() {
        MonitorRepository repository = mock(MonitorRepository.class);
        AtomicReference<String> capturedSql = new AtomicReference<>();
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            return List.of();
        });

        MonitorTaskQueryRepositoryServiceImpl service = new MonitorTaskQueryRepositoryServiceImpl(repository);
        service.listTaskMonitorItems(LocalDateTime.now(), 20, 0, "running", "dubbing", "speaker", "task", "created_desc");

        assertThat(capturedSql.get())
                .contains("FROM task t", "LEFT JOIN video_info", "LEFT JOIN submitter_video")
                .doesNotContain("asr_segment", "translator_segment", "translator_api_task", "speaker_segment")
                .doesNotContain("LEFT JOIN downloader", "LEFT JOIN demucs", "LEFT JOIN whisper", "LEFT JOIN uploader");
    }

    @Test
    void progressScopesEveryChildAggregationToOneTask() {
        MonitorRepository repository = mock(MonitorRepository.class);
        AtomicReference<String> capturedSql = new AtomicReference<>();
        AtomicReference<Object[]> capturedArgs = new AtomicReference<>();
        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenAnswer(invocation -> {
            Object[] args = Arrays.copyOfRange(invocation.getArguments(), 2, invocation.getArguments().length);
            if (args.length > 0 && ("translator_chunk".equals(args[0]) || "translator_api_task".equals(args[0]))) {
                return 1;
            }
            return 0;
        });
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            capturedArgs.set(Arrays.copyOfRange(invocation.getArguments(), 2, invocation.getArguments().length));
            return List.of();
        });

        MonitorTaskQueryRepositoryServiceImpl service = new MonitorTaskQueryRepositoryServiceImpl(repository);
        assertThat(service.findTaskProgress("task-1", LocalDateTime.now())).isNull();

        String sql = capturedSql.get();
        String normalizedSql = sql.replaceAll("\\s+", " ");
        assertThat(sql).doesNotContain("__");
        assertThat(normalizedSql).contains("FROM asr_segment WHERE task_id = ?");
        assertThat(normalizedSql).contains("FROM translator_segment WHERE task_id = ?");
        assertThat(normalizedSql).contains("FROM `translator_chunk` ch");
        assertThat(normalizedSql).doesNotContain("FROM `translator-chunk` ch");
        assertThat(normalizedSql).contains("ch.task_id = ? AND ch.row_role = 'normal'");
        assertThat(normalizedSql).contains("FROM speaker_segment WHERE task_id = ?");
        assertThat(normalizedSql).contains("WHERE t.id = ?");
        assertThat(normalizedSql).contains("FROM uploader_task_status GROUP BY task_id");
        assertThat(normalizedSql).contains("us.youtube_upload_status");
        assertThat(normalizedSql).contains("us.x_upload_status");
        assertThat(normalizedSql).contains("SUM(CASE WHEN COALESCE(NULLIF(status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END) uploader_total_count");
        assertThat(capturedArgs.get()).containsExactly(
                "task-1", "task-1", "task-1", "task-1", "task-1", "task-1", 1, 0
        );
    }

    @Test
    void progressFallsBackToTranslatorJobsWhenApiTaskTableDoesNotExist() {
        MonitorRepository repository = mock(MonitorRepository.class);
        AtomicReference<String> capturedSql = new AtomicReference<>();
        AtomicReference<Object[]> capturedArgs = new AtomicReference<>();
        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenAnswer(invocation -> {
            Object[] args = Arrays.copyOfRange(invocation.getArguments(), 2, invocation.getArguments().length);
            if (args.length > 0 && ("translator_chunk".equals(args[0]) || "translator_jobs".equals(args[0]))) {
                return 1;
            }
            return 0;
        });
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            capturedArgs.set(Arrays.copyOfRange(invocation.getArguments(), 2, invocation.getArguments().length));
            return List.of();
        });

        MonitorTaskQueryRepositoryServiceImpl service = new MonitorTaskQueryRepositoryServiceImpl(repository);
        assertThat(service.findTaskProgress("task-1", LocalDateTime.now())).isNull();

        String normalizedSql = capturedSql.get().replaceAll("\\s+", " ");
        assertThat(normalizedSql).contains("FROM `translator_jobs`");
        assertThat(normalizedSql).doesNotContain("FROM translator_api_task");
        assertThat(normalizedSql).contains("WHERE task_id = ? AND status = 'failed' AND request_key LIKE 'chunk:%'");
        assertThat(capturedArgs.get()).containsExactly(
                "task-1", "task-1", "task-1", "task-1", "task-1", "task-1", 1, 0
        );
    }
}
