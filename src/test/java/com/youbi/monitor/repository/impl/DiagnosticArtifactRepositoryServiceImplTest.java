package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.DiagnosticArtifactRepository;
import com.youbi.monitor.repository.RowMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DiagnosticArtifactRepositoryServiceImplTest {

    @Test
    void listByTaskIdIncludesDiagnosticsFromOperatorAndPublisherRuns() {
        DiagnosticArtifactRepository repository = mock(DiagnosticArtifactRepository.class);
        when(repository.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of());
        DiagnosticArtifactRepositoryServiceImpl service = new DiagnosticArtifactRepositoryServiceImpl(repository);

        service.listByTaskId("pipeline-task");

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
        verify(repository).query(sql.capture(), any(RowMapper.class), arguments.capture());
        assertThat(sql.getValue())
                .contains("FROM operator_task operator_task")
                .contains("operator_task.run_id COLLATE utf8mb4_unicode_ci = diagnostic.task_id")
                .contains("JOIN publisher_jobs publisher_job")
                .contains("JSON_VALID(operator_task.request_json)")
                .contains("'$.prompt'");
        assertThat(arguments.getValue()).containsExactly("pipeline-task", "pipeline-task", "pipeline-task");
    }
}
