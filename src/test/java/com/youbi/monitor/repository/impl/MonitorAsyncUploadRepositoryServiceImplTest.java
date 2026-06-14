package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.MonitorAsyncUploadRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MonitorAsyncUploadRepositoryServiceImplTest {

    @Test
    void markSuccessCanRecoverAStaleTimeoutResult() {
        MonitorAsyncUploadRepository repository = mock(MonitorAsyncUploadRepository.class);
        when(repository.update(any(String.class), eq("{}"), eq("upload-task"))).thenReturn(1);
        MonitorAsyncUploadRepositoryServiceImpl service = new MonitorAsyncUploadRepositoryServiceImpl(repository);

        assertThat(service.markSuccess("upload-task", "{}")).isTrue();

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(repository).update(sql.capture(), eq("{}"), eq("upload-task"));
        assertThat(sql.getValue())
                .contains("status = 'running'")
                .contains("error_code = 'MONITOR_UPLOAD_TIMEOUT'");
    }
}
