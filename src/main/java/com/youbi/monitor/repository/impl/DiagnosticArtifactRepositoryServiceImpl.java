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
    }

    @Override
    public List<DiagnosticArtifactRecord> listByTaskId(String taskId) {
        return repository.query("""
                SELECT diagnostic.id, diagnostic.task_id, diagnostic.run_id, diagnostic.platform,
                       diagnostic.source, diagnostic.account_key,
                       COALESCE(
                           publisher_job.job_name,
                           CASE
                               WHEN JSON_UNQUOTE(JSON_EXTRACT(
                                      IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                                      '$.prompt'
                                    )) LIKE CONCAT(narration.cover_prompt, '%')
                                   THEN 'generate_cover_image'
                               WHEN JSON_UNQUOTE(JSON_EXTRACT(
                                      IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                                      '$.prompt'
                                    )) LIKE CONCAT(narration.background_prompt, '%')
                                   THEN 'generate_background_image'
                               ELSE NULL
                           END
                       ) publisher_job_name,
                       COALESCE(
                           JSON_UNQUOTE(JSON_EXTRACT(
                               IF(JSON_VALID(publisher_job.input_json), publisher_job.input_json, '{}'),
                               '$.aspect_ratio'
                           )),
                           CASE
                               WHEN JSON_UNQUOTE(JSON_EXTRACT(
                                      IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                                      '$.prompt'
                                    )) LIKE CONCAT(narration.cover_prompt, '%')
                                   THEN '1:1'
                               WHEN JSON_UNQUOTE(JSON_EXTRACT(
                                      IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                                      '$.prompt'
                                    )) LIKE CONCAT(narration.background_prompt, '%')
                                   THEN '4:3'
                               ELSE NULL
                           END
                       ) aspect_ratio,
                       diagnostic.step_index, diagnostic.step_name,
                       diagnostic.screenshot_url, diagnostic.html_url,
                       diagnostic.screenshot_size_bytes, diagnostic.html_size_bytes,
                       diagnostic.screenshot_width, diagnostic.screenshot_height,
                       diagnostic.status, diagnostic.error_message, diagnostic.created_at
                FROM uploader_diagonostic diagnostic
                LEFT JOIN operator_task publisher_operator
                  ON publisher_operator.run_id COLLATE utf8mb4_unicode_ci = diagnostic.task_id
                LEFT JOIN product_narration narration
                  ON narration.task_id = ?
                LEFT JOIN publisher_jobs publisher_job
                  ON publisher_job.task_id = ?
                 AND JSON_UNQUOTE(JSON_EXTRACT(
                       IF(JSON_VALID(publisher_job.input_json), publisher_job.input_json, '{}'),
                       '$.prompt'
                     )) COLLATE utf8mb4_unicode_ci
                     = JSON_UNQUOTE(JSON_EXTRACT(
                       IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                       '$.prompt'
                     )) COLLATE utf8mb4_unicode_ci
                WHERE diagnostic.task_id = ?
                   OR EXISTS (
                       SELECT 1
                       FROM operator_task operator_task
                       WHERE operator_task.task_id = ?
                         AND operator_task.run_id COLLATE utf8mb4_unicode_ci = diagnostic.task_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM operator_task operator_task
                       JOIN publisher_jobs publisher_job
                         ON publisher_job.task_id = ?
                        AND (
                            JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(publisher_job.input_json), publisher_job.input_json, '{}'), '$.cover_prompt'))
                                COLLATE utf8mb4_unicode_ci
                                = JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(operator_task.request_json), operator_task.request_json, '{}'), '$.prompt'))
                                    COLLATE utf8mb4_unicode_ci
                            OR JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(publisher_job.input_json), publisher_job.input_json, '{}'), '$.background_prompt'))
                                COLLATE utf8mb4_unicode_ci
                                = JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(operator_task.request_json), operator_task.request_json, '{}'), '$.prompt'))
                                    COLLATE utf8mb4_unicode_ci
                            OR JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(publisher_job.input_json), publisher_job.input_json, '{}'), '$.prompt'))
                                COLLATE utf8mb4_unicode_ci
                                = JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(operator_task.request_json), operator_task.request_json, '{}'), '$.prompt'))
                                    COLLATE utf8mb4_unicode_ci
                        )
                       WHERE operator_task.run_id COLLATE utf8mb4_unicode_ci = diagnostic.task_id
                   )
                   OR (
                       publisher_operator.run_id IS NOT NULL
                       AND (
                           JSON_UNQUOTE(JSON_EXTRACT(
                               IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                               '$.prompt'
                           )) LIKE CONCAT(narration.cover_prompt, '%')
                           OR JSON_UNQUOTE(JSON_EXTRACT(
                               IF(JSON_VALID(publisher_operator.request_json), publisher_operator.request_json, '{}'),
                               '$.prompt'
                           )) LIKE CONCAT(narration.background_prompt, '%')
                       )
                   )
                ORDER BY diagnostic.created_at DESC, diagnostic.run_id DESC,
                         diagnostic.step_index ASC, diagnostic.id ASC
                """, (rs, rowNum) -> mapRecord(rs), taskId, taskId, taskId, taskId, taskId);
    }

    @Override
    public List<DiagnosticArtifactRecord> listByTaskIdAndRunId(String taskId, String runId) {
        return repository.query("""
                SELECT id, task_id, run_id, platform, source, account_key,
                       NULL publisher_job_name, NULL aspect_ratio, step_index, step_name,
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
                rs.getString("publisher_job_name"),
                rs.getString("aspect_ratio"),
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
