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
                .contains("SELECT CONVERT(? USING utf8mb4) COLLATE utf8mb4_unicode_ci task_id")
                .contains("FROM operator_task operator_task")
                .contains("SELECT operator_task.run_id COLLATE utf8mb4_unicode_ci")
                .contains("JOIN publisher_jobs publisher_job")
                .contains("JOIN product_narration narration")
                .contains("LIKE CONCAT(narration.cover_prompt, '%')")
                .contains("LIKE CONCAT(narration.background_prompt, '%')")
                .contains("JSON_VALID(operator_task.request_json)")
                .contains("'$.prompt'")
                .contains("ON diagnostic.task_id = relevant_task.task_id")
                .doesNotContain("publisher_job.job_name")
                .doesNotContain("'$.aspect_ratio'");
        assertThat(arguments.getValue()).containsExactly(
                "pipeline-task", "pipeline-task", "pipeline-task", "pipeline-task"
        );
    }
}
