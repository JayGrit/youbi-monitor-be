package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.ITaskLifecycleRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.service.MonitorService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class TaskLifecycleRepositoryServiceImpl extends MonitorRepositorySqlSupport implements ITaskLifecycleRepositoryService {
    public TaskLifecycleRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    @Transactional
    
    public boolean markTaskReady(String taskId) {
        RetryStage failedStage = findFailedStage(taskId);
        if (failedStage == null) {
            return false;
        }

        applyStagedPipelineRetry(taskId);
        resetFailedStage(taskId, failedStage);
        resetDownstreamStages(taskId, failedStage);
        resetStageChildren(taskId, failedStage);
        resetExhaustedTranslatorApiTasks(taskId);
        repository.update("""
                UPDATE task
                SET status = 'ready',
                    current_stage = ?,
                    completed_at = NULL,
                    error_message = NULL
                WHERE id = ?
                """, failedStage.key(), taskId);
        return true;
    }

    @Override
    public String findTaskStatus(String taskId) {
        List<String> statuses = repository.queryForList(
                "SELECT status FROM task WHERE id = ?",
                String.class,
                taskId
        );
        return statuses.isEmpty() ? null : statuses.get(0);
    }

    @Transactional
    
    public MonitorService.TaskStopResult stopTask(String taskId) {
        List<String> statuses = repository.queryForList(
                "SELECT status FROM task WHERE id = ?",
                String.class,
                taskId
        );
        if (statuses.isEmpty()) {
            return null;
        }

        String message = "手动停止任务";
        int stoppedStages = 0;
        String stoppedStage = "";
        for (RetryStage stage : RETRY_STAGES) {
            if (!tableExists(stage.table())) {
                continue;
            }
            ensureOperatorColumn(stage.table());
            int updated = repository.update("""
                    UPDATE %s
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?,
                        `operator` = NULL
                    WHERE task_id = ? AND status = 'running'
                    """.formatted(quotedIdentifier(stage.table())), message, taskId);
            if (updated > 0) {
                stoppedStages += updated;
                if (stoppedStage.isBlank()) {
                    stoppedStage = stage.key();
                }
            }
        }

        if (tableExists("translator_api_task")) {
            ensureOperatorColumn("translator_api_task");
            stoppedStages += repository.update("""
                    UPDATE translator_api_task
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?,
                        `operator` = NULL
                    WHERE task_id = ? AND status IN ('pending', 'running')
                    """, message, taskId);
        }
        if (tableExists("speaker_segment")) {
            ensureOperatorColumn("speaker_segment");
            stoppedStages += repository.update("""
                    UPDATE speaker_segment
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?,
                        `operator` = NULL
                    WHERE task_id = ? AND status IN ('ready', 'running')
                    """, message, taskId);
        }

        if (stoppedStages == 0 && !"running".equals(statuses.get(0))) {
            return new MonitorService.TaskStopResult(statuses.get(0), 0, false);
        }

        repository.update("""
                UPDATE task
                SET status = 'failed',
                    current_stage = COALESCE(NULLIF(?, ''), current_stage),
                    completed_at = NOW(),
                    error_message = ?
                WHERE id = ?
                """, stoppedStage, message, taskId);
        applyStagedPipelineFailure(taskId, statuses.get(0));
        return new MonitorService.TaskStopResult("failed", stoppedStages, true);
    }

    private RetryStage findFailedStage(String taskId) {
        if (hasExhaustedTranslatorApiTasks(taskId)) {
            return RETRY_STAGES.stream()
                    .filter(stage -> "translator".equals(stage.key()))
                    .findFirst()
                    .orElse(null);
        }

        for (RetryStage stage : RETRY_STAGES) {
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM " + stage.table() + " WHERE task_id = ? AND status = 'failed'",
                    Integer.class,
                    taskId
            );
            if (count != null && count > 0) {
                return stage;
            }
        }

        List<String> currentStages = repository.queryForList("""
                SELECT current_stage
                FROM task
                WHERE id = ? AND status = 'failed'
                """, String.class, taskId);
        if (currentStages.isEmpty()) {
            return null;
        }
        String currentStage = currentStages.get(0);
        for (RetryStage stage : RETRY_STAGES) {
            if (stage.key().equals(currentStage)) {
                return stage;
            }
        }
        return null;
    }

    private boolean hasExhaustedTranslatorApiTasks(String taskId) {
        if (!tableExists("translator_api_task")) {
            return false;
        }
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM translator_api_task
                WHERE task_id = ?
                  AND status = 'failed'
                  AND attempt_count >= max_attempts
                """, Integer.class, taskId);
        return count != null && count > 0;
    }

    private void resetFailedStage(String taskId, RetryStage stage) {
        ensureOperatorColumn(stage.table());
        repository.update("""
                UPDATE %s
                SET status = 'ready',
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id = ?
                """.formatted(stage.table()), taskId);
    }

    private void resetDownstreamStages(String taskId, RetryStage failedStage) {
        int failedIndex = RETRY_STAGES.indexOf(failedStage);
        for (int i = failedIndex + 1; i < RETRY_STAGES.size(); i++) {
            RetryStage stage = RETRY_STAGES.get(i);
            ensureOperatorColumn(stage.table());
            repository.update("""
                    UPDATE %s
                    SET status = 'pending',
                        started_at = NULL,
                        completed_at = NULL,
                        error_message = NULL,
                        `operator` = NULL
                    WHERE task_id = ?
                    """.formatted(stage.table()), taskId);
        }
    }

    private void resetStageChildren(String taskId, RetryStage failedStage) {
        if ("translator".equals(failedStage.key())) {
            if (tableExists("translator_api_task")) {
                ensureOperatorColumn("translator_api_task");
                repository.update("""
                        UPDATE translator_api_task
                        SET status = 'pending',
                            attempt_count = 0,
                            started_at = NULL,
                            completed_at = NULL,
                            error_message = NULL,
                            response_json = NULL,
                            next_run_at = NOW(),
                            `operator` = NULL
                        WHERE task_id = ? AND status IN ('failed', 'running')
                        """, taskId);
            }
            return;
        }

        if ("speaker".equals(failedStage.key()) && tableExists("speaker_segment")) {
            ensureOperatorColumn("speaker_segment");
            repository.update("""
                    UPDATE speaker_segment
                    SET status = 'ready',
                        attempt_count = 0,
                        started_at = NULL,
                        completed_at = NULL,
                        error_message = NULL,
                        `operator` = NULL
                    WHERE task_id = ? AND status = 'failed'
                    """, taskId);
        }

        if ("uploader".equals(failedStage.key())) {
            UPLOADER_TASK_TABLES.forEach((platform, table) -> {
                if (!tableExists(table)) {
                    return;
                }
                List<UploadAccountStatusChange> accountStatusChanges = repository.query("""
                        SELECT submission.task_id, submission.account_key, submission.status
                        FROM %s submission
                        JOIN video_info video_info ON video_info.task_id = submission.task_id
                        LEFT JOIN uploader uploader ON uploader.task_id = submission.task_id
                        WHERE submission.task_id = ?
                          AND submission.status IN ('failed', 'running')
                          AND submission.account_key = video_info.type
                          AND (
                              COALESCE(NULLIF(uploader.upload_platforms, ''), '') = ''
                              OR FIND_IN_SET(?, REPLACE(uploader.upload_platforms, ' ', '')) > 0
                          )
                        FOR UPDATE
                        """.formatted(quotedIdentifier(table)),
                        (rs, rowNum) -> new UploadAccountStatusChange(
                                rs.getString("task_id"),
                                rs.getString("account_key"),
                                rs.getString("status"),
                                "ready"
                        ),
                        taskId,
                        platform
                );
                repository.update("""
                        UPDATE %s submission
                        JOIN video_info video_info ON video_info.task_id = submission.task_id
                        LEFT JOIN uploader uploader ON uploader.task_id = submission.task_id
                        SET submission.status = 'ready',
                            submission.started_at = NULL,
                            submission.completed_at = NULL,
                            submission.error_message = NULL
                        WHERE submission.task_id = ?
                          AND submission.status IN ('failed', 'running')
                          AND submission.account_key = video_info.type
                          AND (
                              COALESCE(NULLIF(uploader.upload_platforms, ''), '') = ''
                              OR FIND_IN_SET(?, REPLACE(uploader.upload_platforms, ' ', '')) > 0
                          )
                        """.formatted(quotedIdentifier(table)), taskId, platform);
                applyUploaderAccountStatusChanges(platform, accountStatusChanges);
                repository.update("""
                        UPDATE uploader
                        SET %s = 'ready'
                        WHERE task_id = ?
                        """.formatted(quotedIdentifier(uploadStatusColumn(platform))), taskId);
            });
        }
    }

    private void resetExhaustedTranslatorApiTasks(String taskId) {
        if (!tableExists("translator_api_task")) {
            return;
        }
        ensureOperatorColumn("translator_api_task");
        repository.update("""
                UPDATE translator_api_task
                SET status = 'pending',
                    attempt_count = 0,
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    response_json = NULL,
                    next_run_at = NOW(),
                    `operator` = NULL
                WHERE task_id = ?
                  AND status = 'failed'
                  AND attempt_count >= max_attempts
                """, taskId);
    }

    @Override
    public boolean hasRunningStage(String taskId) {
        for (RetryStage stage : RETRY_STAGES) {
            if (!tableExists(stage.table())) {
                continue;
            }
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM " + quotedIdentifier(stage.table()) + " WHERE task_id = ? AND status = 'running'",
                    Integer.class,
                    taskId
            );
            if (count != null && count > 0) {
                return true;
            }
        }
        if (tableExists("speaker_segment")) {
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM speaker_segment WHERE task_id = ? AND status = 'running'",
                    Integer.class,
                    taskId
            );
            return count != null && count > 0;
        }
        return false;
    }

    @Override
    public void resetTaskRowsForDownloader(String taskId) {
        for (String table : RESET_CHILD_TABLES) {
            if (tableExists(table)) {
                repository.update("DELETE FROM " + quotedIdentifier(table) + " WHERE task_id = ?", taskId);
            }
        }

        resetVideoInfo(taskId);
        for (RetryStage stage : RETRY_STAGES) {
            resetStageRow(taskId, stage, "downloader".equals(stage.key()) ? "ready" : "pending");
        }

        repository.update("""
                UPDATE task
                SET status = 'ready',
                    current_stage = 'downloader',
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL
                WHERE id = ?
                """, taskId);
        reconcileUploaderAccountStagedCounts();
    }

    @Override
    public int deleteTaskRows(String taskId) {
        int deleted = 0;
        for (String table : taskScopedTables()) {
            deleted += repository.update("DELETE FROM " + quotedIdentifier(table) + " WHERE task_id = ?", taskId);
        }
        deleted += repository.update("DELETE FROM task WHERE id = ?", taskId);
        reconcileUploaderAccountStagedCounts();
        return deleted;
    }

    @Override
    public MonitorService.DownloaderFailureList listFailedTasks() {
        if (!tableExists("downloader_submission")
                || !tableExists("task")) {
            return new MonitorService.DownloaderFailureList(0, List.of());
        }
        List<MonitorService.DownloaderFailure> rows = repository.query("""
                SELECT
                    submission.id submission_id,
                    submission.task_id,
                    COALESCE(NULLIF(source_video.title, ''), submission.task_id) title,
                    submission.type,
                    NULLIF(task.error_message, '') error_message,
                    task.completed_at,
                    submission.source_url
                FROM downloader_submission submission
                JOIN task task ON task.id = submission.task_id
                LEFT JOIN video_info video_info ON video_info.task_id = submission.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                WHERE submission.status = 'success'
                  AND task.status = 'failed'
                ORDER BY task.completed_at DESC, submission.id DESC
                """, (rs, rowNum) -> new MonitorService.DownloaderFailure(
                rs.getLong("submission_id"),
                rs.getString("task_id"),
                rs.getString("title"),
                rs.getString("type"),
                rs.getString("error_message"),
                timestamp(rs, "completed_at"),
                rs.getString("source_url")
        ));
        return new MonitorService.DownloaderFailureList(rows.size(), rows);
    }

    @Override
    @Transactional
    public MonitorService.DownloaderRollbackDatabaseResult rollbackFailedTasks(List<Long> submissionIds) {
        List<Long> normalizedIds = submissionIds == null ? List.of() : submissionIds.stream()
                .filter(id -> id != null && id > 0)
                .distinct()
                .toList();
        if (normalizedIds.isEmpty()) {
            throw new IllegalArgumentException("No failed task selected.");
        }

        String placeholders = placeholders(normalizedIds.size());
        List<Map<String, Object>> candidates = repository.query("""
                SELECT submission.id, submission.task_id
                FROM downloader_submission submission
                JOIN task task ON task.id = submission.task_id
                WHERE submission.id IN (%s)
                  AND submission.status = 'success'
                  AND task.status = 'failed'
                FOR UPDATE
                """.formatted(placeholders), (rs, rowNum) -> Map.of(
                "submissionId", rs.getLong("id"),
                "taskId", rs.getString("task_id")
        ), normalizedIds.toArray());
        if (candidates.size() != normalizedIds.size()) {
            throw new IllegalArgumentException("Some selected failed tasks no longer exist or are not rollbackable.");
        }

        List<String> taskTables = taskScopedTablesForRollback();
        int deletedRows = 0;
        for (Map<String, Object> candidate : candidates) {
            String taskId = stringValue(candidate.get("taskId"));
            for (String table : taskTables) {
                deletedRows += repository.update(
                        "DELETE FROM " + quotedIdentifier(table) + " WHERE task_id = ?",
                        taskId
                );
            }
            deletedRows += repository.update("DELETE FROM task WHERE id = ?", taskId);
        }

        int restored = repository.update("""
                UPDATE downloader_submission
                SET task_id = NULL,
                    status = 'ready',
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE id IN (%s)
                """.formatted(placeholders), normalizedIds.toArray());
        reconcileDownloaderPendingCounts();
        reconcileUploaderAccountStagedCounts();
        return new MonitorService.DownloaderRollbackDatabaseResult(restored, deletedRows);
    }

    private List<String> taskScopedTablesForRollback() {
        if (!tableExists("downloader_submission")) {
            return List.of();
        }
        List<String> tables = repository.queryForList("""
                SELECT DISTINCT columns_info.TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS columns_info
                JOIN INFORMATION_SCHEMA.TABLES tables_info
                  ON tables_info.TABLE_SCHEMA = columns_info.TABLE_SCHEMA
                 AND tables_info.TABLE_NAME = columns_info.TABLE_NAME
                WHERE columns_info.TABLE_SCHEMA = DATABASE()
                  AND columns_info.COLUMN_NAME = 'task_id'
                  AND columns_info.TABLE_NAME <> 'downloader_submission'
                  AND tables_info.TABLE_TYPE = 'BASE TABLE'
                ORDER BY CASE
                    WHEN columns_info.TABLE_NAME IN ('translator_api_task', 'speaker_segment', 'asr_segment') THEN 0
                    WHEN columns_info.TABLE_NAME LIKE 'yd\\_%' THEN 1
                    ELSE 2
                  END,
                  columns_info.TABLE_NAME
                """, String.class);
        return new ArrayList<>(tables);
    }

    private void reconcileDownloaderPendingCounts() {
        if (!tableExists("uploader_account")
                || !tableExists("downloader_submission")
                || !columnExists("uploader_account", "downloader_pending_count")) {
            return;
        }
        repository.update("""
                UPDATE uploader_account account
                LEFT JOIN (
                    SELECT type account_key, COUNT(*) pending_count
                    FROM downloader_submission
                    WHERE status = 'ready'
                      AND NULLIF(type, '') IS NOT NULL
                    GROUP BY type
                ) pending ON pending.account_key = account.account_key
                SET account.downloader_pending_count = COALESCE(pending.pending_count, 0),
                    account.metrics_updated_at = NOW(),
                    account.updated_at = NOW()
                """);
    }

    private List<String> taskScopedTables() {
        return repository.queryForList("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND COLUMN_NAME = 'task_id'
                  AND (TABLE_NAME LIKE 'yd\\_%' OR TABLE_NAME = 'downloader_submission')
                ORDER BY CASE
                    WHEN TABLE_NAME IN ('translator_api_task', 'speaker_segment', 'asr_segment') THEN 0
                    WHEN TABLE_NAME = 'downloader_submission' THEN 2
                    ELSE 1
                  END,
                  TABLE_NAME
                """, String.class);
    }

    private void resetVideoInfo(String taskId) {
        if (!tableExists("video_info")) {
            return;
        }
        List<String> resetColumns = resettableColumns("video_info", PRESERVED_VIDEO_INFO_COLUMNS);
        if (resetColumns.isEmpty()) {
            restoreVideoInfoSource(taskId);
            restoreVideoInfoProcessingOptions(taskId);
            return;
        }
        repository.update("UPDATE video_info SET " + nullAssignments(resetColumns) + " WHERE task_id = ?", taskId);
        restoreVideoInfoSource(taskId);
        restoreVideoInfoProcessingOptions(taskId);
    }

    private void restoreVideoInfoSource(String taskId) {
        repository.update("""
                INSERT INTO video_info (task_id, source_url)
                SELECT task_id, source_url
                FROM downloader_submission
                WHERE task_id = ?
                ORDER BY id DESC
                LIMIT 1
                ON DUPLICATE KEY UPDATE
                    source_url = COALESCE(video_info.source_url, VALUES(source_url))
                """, taskId);
    }

    private void restoreVideoInfoProcessingOptions(String taskId) {
        if (tableExists("downloader_submission")) {
            repository.update("""
                    UPDATE video_info video_info
                    JOIN downloader_submission submission ON submission.task_id = video_info.task_id
                    SET video_info.type = COALESCE(video_info.type, submission.type),
                        video_info.need_subtitle = COALESCE(video_info.need_subtitle, submission.need_subtitle),
                        video_info.need_dubbing = COALESCE(video_info.need_dubbing, submission.need_dubbing),
                        video_info.need_separation = COALESCE(video_info.need_separation, submission.need_separation)
                    WHERE video_info.task_id = ?
                    """, taskId);
        }
        if (tableExists("submitter_author")) {
            repository.update("""
                    UPDATE video_info video_info
                    JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                    JOIN submitter_author author
                      ON author.author = COALESCE(
                           NULLIF(source_video.uploader, ''),
                           NULLIF(source_video.import_author, ''),
                           NULLIF(source_video.channel_id, '')
                         )
                     AND author.type = video_info.type
                    SET video_info.need_subtitle = COALESCE(video_info.need_subtitle, author.need_subtitle),
                        video_info.need_dubbing = COALESCE(video_info.need_dubbing, author.need_dubbing),
                        video_info.need_separation = COALESCE(video_info.need_separation, author.need_separation),
                        video_info.source_language = COALESCE(video_info.source_language, author.source_language),
                        video_info.target_language = COALESCE(video_info.target_language, author.target_language)
                    WHERE video_info.task_id = ?
                    """, taskId);
        }
    }

    private void resetStageRow(String taskId, RetryStage stage, String status) {
        if (!tableExists(stage.table())) {
            return;
        }
        ensureOperatorColumn(stage.table());
        List<String> resetColumns = resettableColumns(stage.table(), SYSTEM_STAGE_COLUMNS);
        String extraAssignments = resetColumns.isEmpty() ? "" : ",\n                    " + nullAssignments(resetColumns);
        repository.update("""
                UPDATE %s
                SET status = ?,
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL%s
                WHERE task_id = ?
                """.formatted(quotedIdentifier(stage.table()), extraAssignments), status, taskId);
    }

    private List<String> resettableColumns(String table, List<String> preservedColumns) {
        return repository.queryForList("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = ?
                          AND IS_NULLABLE = 'YES'
                        """, String.class, table)
                .stream()
                .filter(column -> !preservedColumns.contains(column))
                .toList();
    }

    private static String nullAssignments(List<String> columns) {
        return columns.stream()
                .map(column -> quotedIdentifier(column) + " = NULL")
                .reduce((left, right) -> left + ",\n                    " + right)
                .orElse("");
    }


}
