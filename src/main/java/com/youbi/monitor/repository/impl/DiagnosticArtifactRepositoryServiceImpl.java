package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.DiagnosticArtifactRepository;
import com.youbi.monitor.repository.IDiagnosticArtifactRepositoryService;
import org.springframework.stereotype.Service;

@Service
public class DiagnosticArtifactRepositoryServiceImpl implements IDiagnosticArtifactRepositoryService {
    private final DiagnosticArtifactRepository repository;

    public DiagnosticArtifactRepositoryServiceImpl(DiagnosticArtifactRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
    }

    @Override
    public Long insertUploadedArtifact(
            String taskId,
            String runId,
            String platform,
            String source,
            String accountKey,
            int stepIndex,
            String stepName,
            String screenshotUrl,
            String htmlUrl,
            Long screenshotSizeBytes,
            Long htmlSizeBytes,
            Integer screenshotWidth,
            Integer screenshotHeight
    ) {
        return repository.insertAndReturnKey("""
                INSERT INTO uploader_diagonostic
                (task_id, run_id, platform, source, account_key, step_index, step_name,
                 screenshot_url, html_url, screenshot_size_bytes, html_size_bytes,
                 screenshot_width, screenshot_height, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploaded')
                """,
                taskId,
                runId,
                platform,
                source,
                emptyToNull(accountKey),
                stepIndex,
                stepName,
                screenshotUrl,
                htmlUrl,
                screenshotSizeBytes,
                htmlSizeBytes,
                screenshotWidth,
                screenshotHeight
        );
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
