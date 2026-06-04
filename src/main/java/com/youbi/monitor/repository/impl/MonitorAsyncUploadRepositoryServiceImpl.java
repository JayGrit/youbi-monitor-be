package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.MonitorUploadTaskRow;
import com.youbi.monitor.repository.IMonitorAsyncUploadRepositoryService;
import com.youbi.monitor.repository.MonitorAsyncUploadRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class MonitorAsyncUploadRepositoryServiceImpl implements IMonitorAsyncUploadRepositoryService {
    private static final String TABLE = "monitor_upload_task";

    private final MonitorAsyncUploadRepository repository;

    public MonitorAsyncUploadRepositoryServiceImpl(MonitorAsyncUploadRepository repository) {
        this.repository = repository;
    }

    @Override
    public void ensureSchema() {
    }

    @Override
    public long countActiveTasks() {
        return repository.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE status IN ('accepted', 'running')",
                Long.class
        );
    }

    @Override
    public void insertAcceptedTask(String uploadTaskId, String platform, String upstreamTaskId, String accountKey, String requestJson, String videoUrl) {
        repository.update(
                """
                INSERT INTO monitor_upload_task (
                    upload_task_id, platform, upstream_task_id, account_key, status,
                    request_json, video_url, started_at
                )
                VALUES (?, ?, NULLIF(?, ''), ?, 'accepted', ?, ?, NOW())
                """,
                uploadTaskId,
                platform,
                upstreamTaskId,
                accountKey,
                requestJson,
                videoUrl
        );
    }

    @Override
    public Optional<MonitorUploadTaskRow> findByUploadTaskId(String uploadTaskId) {
        List<MonitorUploadTaskRow> rows = repository.query(
                "SELECT * FROM " + TABLE + " WHERE upload_task_id = ?",
                (rs, rowNum) -> new MonitorUploadTaskRow(
                        rs.getString("upload_task_id"),
                        rs.getString("platform"),
                        rs.getString("upstream_task_id"),
                        rs.getString("account_key"),
                        rs.getString("status"),
                        rs.getString("result_json"),
                        rs.getString("error_code"),
                        rs.getString("error_message"),
                        rs.getString("video_url"),
                        toLocalDateTime(rs.getTimestamp("started_at")),
                        toLocalDateTime(rs.getTimestamp("completed_at"))
                ),
                uploadTaskId
        );
        return rows.stream().findFirst();
    }

    @Override
    public boolean markRunning(String uploadTaskId) {
        int started = repository.update(
                "UPDATE " + TABLE + " SET status = 'running', started_at = COALESCE(started_at, NOW()), error_message = NULL WHERE upload_task_id = ? AND status = 'accepted'",
                uploadTaskId
        );
        return started > 0;
    }

    @Override
    public boolean markSuccess(String uploadTaskId, String resultJson) {
        int updated = repository.update(
                "UPDATE " + TABLE + " SET status = 'success', result_json = ?, error_code = NULL, error_message = NULL, completed_at = NOW() WHERE upload_task_id = ? AND status = 'running'",
                resultJson,
                uploadTaskId
        );
        return updated > 0;
    }

    @Override
    public boolean markFailed(String uploadTaskId, String resultJson, String errorCode, String errorMessage) {
        int updated = repository.update(
                "UPDATE " + TABLE + " SET status = 'failed', result_json = ?, error_code = ?, error_message = ?, completed_at = NOW() WHERE upload_task_id = ? AND status = 'running'",
                resultJson,
                errorCode,
                errorMessage,
                uploadTaskId
        );
        return updated > 0;
    }

    @Override
    public void failStaleRunningTasks(String errorMessage, int timeoutSeconds) {
        repository.update("""
                UPDATE monitor_upload_task
                SET status = 'failed',
                    error_code = 'MONITOR_UPLOAD_TIMEOUT',
                    error_message = ?,
                    completed_at = NOW()
                WHERE status IN ('accepted', 'running')
                  AND started_at IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, started_at, NOW()) > ?
                """,
                errorMessage,
                timeoutSeconds);
    }

    private LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
