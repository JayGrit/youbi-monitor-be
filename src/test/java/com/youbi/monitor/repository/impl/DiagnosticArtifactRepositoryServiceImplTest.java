package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.DiagnosticArtifactRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DiagnosticArtifactRepositoryServiceImplTest {

    @Test
    void insertUploadedArtifactWritesLegacyArchiveRecord() {
        DiagnosticArtifactRepository repository = mock(DiagnosticArtifactRepository.class);
        when(repository.insertAndReturnKey(any(String.class), any(Object[].class))).thenReturn(42L);
        DiagnosticArtifactRepositoryServiceImpl service = new DiagnosticArtifactRepositoryServiceImpl(repository);

        Long id = service.insertUploadedArtifact(
                "pipeline-task",
                "archive-run",
                "shipinhao",
                "monitor",
                "",
                3,
                "submitted",
                "https://example.test/screenshot.png",
                "https://example.test/page.html",
                100L,
                200L,
                1280,
                720
        );

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
        verify(repository).insertAndReturnKey(sql.capture(), arguments.capture());
        assertThat(id).isEqualTo(42L);
        assertThat(sql.getValue())
                .contains("INSERT INTO uploader_diagonostic")
                .doesNotContain("operator_task");
        assertThat(arguments.getValue()).containsExactly(
                "pipeline-task",
                "archive-run",
                "shipinhao",
                "monitor",
                null,
                3,
                "submitted",
                "https://example.test/screenshot.png",
                "https://example.test/page.html",
                100L,
                200L,
                1280,
                720
        );
    }
}
