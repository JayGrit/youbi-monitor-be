package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.DiagnosticArtifactRecord;
import com.youbi.monitor.repository.DiagnosticArtifactRepository;
import com.youbi.monitor.repository.IDiagnosticArtifactRepositoryService;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class DiagnosticArtifactRepositoryServiceImpl implements IDiagnosticArtifactRepositoryService {
    private final DiagnosticArtifactRepository repository;

    public DiagnosticArtifactRepositoryServiceImpl(DiagnosticArtifactRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
        repository.execute("""
                CREATE TABLE IF NOT EXISTS uploader_diagonostic (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    task_id VARCHAR(128) NOT NULL,
                    run_id VARCHAR(128) NOT NULL,
                    platform VARCHAR(32) NOT NULL,
                    source VARCHAR(64) NOT NULL,
                    account_key VARCHAR(128) NULL,
                    step_index INT NOT NULL,
                    step_name VARCHAR(128) NOT NULL,
                    screenshot_url TEXT NOT NULL,
                    html_url TEXT NULL,
                    screenshot_size_bytes BIGINT NULL,
                    html_size_bytes BIGINT NULL,
                    screenshot_width INT NULL,
                    screenshot_height INT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'uploaded',
                    error_message TEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    KEY idx_uploader_diag_task_order (task_id, run_id, step_index, id),
                    KEY idx_uploader_diag_platform_time (platform, created_at),
                    KEY idx_uploader_diag_source_time (source, created_at)
                )
                """);
    }

    @Override
    public List<DiagnosticArtifactRecord> listByTaskId(String taskId) {
        return repository.query("""
                SELECT id, task_id, run_id, platform, source, account_key, step_index, step_name,
                       screenshot_url, html_url, screenshot_size_bytes, html_size_bytes,
                       screenshot_width, screenshot_height, status, error_message, created_at
                FROM uploader_diagonostic
                WHERE task_id = ?
                ORDER BY created_at DESC, run_id DESC, step_index ASC, id ASC
                """, (rs, rowNum) -> mapRecord(rs), taskId);
    }

    @Override
    public List<DiagnosticArtifactRecord> listByTaskIdAndRunId(String taskId, String runId) {
        return repository.query("""
                SELECT id, task_id, run_id, platform, source, account_key, step_index, step_name,
                       screenshot_url, html_url, screenshot_size_bytes, html_size_bytes,
                       screenshot_width, screenshot_height, status, error_message, created_at
                FROM uploader_diagonostic
                WHERE task_id = ? AND run_id = ?
                ORDER BY step_index ASC, id ASC
                """, (rs, rowNum) -> mapRecord(rs), taskId, runId);
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

    private DiagnosticArtifactRecord mapRecord(ResultSet rs) throws SQLException {
        return new DiagnosticArtifactRecord(
                rs.getLong("id"),
                rs.getString("task_id"),
                rs.getString("run_id"),
                rs.getString("platform"),
                rs.getString("source"),
                rs.getString("account_key"),
                rs.getInt("step_index"),
                rs.getString("step_name"),
                rs.getString("screenshot_url"),
                rs.getString("html_url"),
                nullableLong(rs, "screenshot_size_bytes"),
                nullableLong(rs, "html_size_bytes"),
                nullableInt(rs, "screenshot_width"),
                nullableInt(rs, "screenshot_height"),
                rs.getString("status"),
                rs.getString("error_message"),
                rs.getObject("created_at", LocalDateTime.class)
        );
    }

    private Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
