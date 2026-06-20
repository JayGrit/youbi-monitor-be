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
    void progressScopesEveryChildAggregationToOneTask() {
        MonitorRepository repository = mock(MonitorRepository.class);
        AtomicReference<String> capturedSql = new AtomicReference<>();
        AtomicReference<Object[]> capturedArgs = new AtomicReference<>();
        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenReturn(0);
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
        assertThat(normalizedSql).contains("ch.task_id = ? AND ch.row_role = 'normal'");
        assertThat(normalizedSql).contains("FROM speaker_segment WHERE task_id = ?");
        assertThat(normalizedSql).contains("WHERE t.id = ?");
        assertThat(capturedArgs.get()).containsExactly(
                "task-1", "task-1", "task-1", "task-1", "task-1", "task-1", "task-1", "task-1", 1, 0
        );
    }
}
