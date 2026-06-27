package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.ITaskLifecycleRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.model.RouteNode;
import com.youbi.monitor.service.MonitorService;
import com.youbi.monitor.service.StageRegistry;
import com.youbi.monitor.service.TaskRouteService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class TaskLifecycleRepositoryServiceImpl extends MonitorRepositorySqlSupport implements ITaskLifecycleRepositoryService {
    private final TaskRouteService taskRouteService;
    private final StageRegistry stageRegistry;

    public TaskLifecycleRepositoryServiceImpl(
            MonitorRepository repository,
            TaskRouteService taskRouteService,
            StageRegistry stageRegistry
    ) {
        super(repository);
        this.taskRouteService = taskRouteService;
        this.stageRegistry = stageRegistry;
    }

    @Transactional
    public boolean markTaskReady(String taskId) {
        List<String> taskStatuses = repository.queryForList(
                "SELECT status FROM task WHERE id = ? FOR UPDATE",
                String.class,
                taskId
        );
        if (taskStatuses.isEmpty() || !"failed".equals(taskStatuses.get(0))) {
            return false;
        }
        if (hasRunningStage(taskId)) {
            throw new IllegalStateException("Task still has a running stage or child job.");
        }

        List<RouteNode> route = taskRouteService.routeForTask(taskId);
        RouteNode failedNode = findFailedNode(taskId, route);
        if (failedNode == null) return false;
        int failedIndex = route.indexOf(failedNode);

        applyStagedPipelineRetry(taskId);
        resetFailedNode(taskId, failedNode);
        resetDownstreamNodes(taskId, route, failedIndex);
        resetStageChildren(taskId, failedNode);
        resetExhaustedTranslatorApiTasks(taskId);
        invalidateRouteLogs(taskId, route.subList(failedIndex, route.size()));
        repository.update("""
                UPDATE task
                SET status = 'ready',
                    current_stage = ?,
                    completed_at = NULL,
                    error_message = NULL
                WHERE id = ?
                """, failedNode.stage(), taskId);
        return true;
    }

    @Override
    public String findTaskStatus(String taskId) {
        List<String> statuses = repository.queryForList(
                "SELECT status FROM task WHERE id = ? FOR UPDATE",
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
        List<RouteNode> route = taskRouteService.routeForTask(taskId);
        for (RouteNode node : route) {
            if (!tableExists(node.tableName())) {
                continue;
            }
            ensureOperatorColumn(node.tableName());
            int updated = repository.update("""
                    UPDATE %s
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?,
                        `operator` = NULL
                    WHERE task_id = ? AND status = 'running'
                    """.formatted(quotedIdentifier(node.tableName())), message, taskId);
            if (updated > 0) {
                stoppedStages += updated;
                if (stoppedStage.isBlank()) {
                    stoppedStage = node.stage();
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
        if (tableExists("asseter_jobs")) {
            stoppedStages += repository.update("""
                    UPDATE asseter_jobs
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?
                    WHERE task_id = ? AND status IN ('pending', 'running')
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

    private RouteNode findFailedNode(String taskId, List<RouteNode> route) {
        if (hasExhaustedTranslatorApiTasks(taskId)) {
            return route.stream()
                    .filter(node -> "translator".equals(node.stage()))
                    .findFirst()
                    .orElse(null);
        }

        for (RouteNode node : route) {
            if (!tableExists(node.tableName())) continue;
            boolean scopedBySubStage = "combiner".equals(node.stage());
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM " + quotedIdentifier(node.tableName())
                            + " WHERE task_id = ? AND status = 'failed'"
                            + (scopedBySubStage ? " AND sub_stage = ?" : ""),
                    Integer.class,
                    scopedBySubStage ? new Object[]{taskId, node.subStage()} : new Object[]{taskId}
            );
            if (count != null && count > 0) return node;
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
        List<RouteNode> candidates = route.stream().filter(node -> node.stage().equals(currentStage)).toList();
        if (candidates.size() == 1) return candidates.get(0);
        if ("combiner".equals(currentStage) && tableExists("combiner")) {
            List<String> subStages = repository.queryForList(
                    "SELECT sub_stage FROM combiner WHERE task_id = ?",
                    String.class,
                    taskId
            );
            if (!subStages.isEmpty()) {
                for (RouteNode candidate : candidates) {
                    if (candidate.subStage().equals(subStages.get(0))) return candidate;
                }
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

    private void resetFailedNode(String taskId, RouteNode node) {
        ensureOperatorColumn(node.tableName());
        boolean setSubStage = "combiner".equals(node.stage());
        List<Object> arguments = new ArrayList<>();
        if (setSubStage) arguments.add(node.subStage());
        arguments.add(taskId);
        repository.update("""
                UPDATE %s
                SET status = 'ready',
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL%s
                WHERE task_id = ?
                """.formatted(
                quotedIdentifier(node.tableName()),
                setSubStage ? ", sub_stage = ?" : ""
        ), arguments.toArray());
    }

    private void resetDownstreamNodes(String taskId, List<RouteNode> route, int failedIndex) {
        Set<String> resetTables = new HashSet<>();
        resetTables.add(route.get(failedIndex).tableName());
        for (int i = failedIndex + 1; i < route.size(); i++) {
            RouteNode node = route.get(i);
            // combiner has one materialized row even when a route contains two combiner nodes.
            if (!resetTables.add(node.tableName()) || !tableExists(node.tableName())) continue;
            ensureOperatorColumn(node.tableName());
            repository.update("""
                    UPDATE %s
                    SET status = 'pending',
                        started_at = NULL,
                        completed_at = NULL,
                        error_message = NULL,
                        `operator` = NULL
                    WHERE task_id = ?
                    """.formatted(quotedIdentifier(node.tableName())), taskId);
        }
    }

    private void resetStageChildren(String taskId, RouteNode failedNode) {
        if ("translator".equals(failedNode.stage())) {
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

        if ("speaker".equals(failedNode.stage()) && tableExists("speaker_segment")) {
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

        if ("asseter".equals(failedNode.stage()) && tableExists("asseter_jobs")) {
            repository.update("""
                    UPDATE asseter_jobs
                    SET status = 'pending',
                        attempts = 0,
                        output_url = NULL,
                        started_at = NULL,
                        completed_at = NULL,
                        error_message = NULL
                    WHERE task_id = ? AND status IN ('failed', 'running')
                    """, taskId);
        }

        if ("uploader".equals(failedNode.stage())) {
            UPLOADER_TASK_TABLES.forEach((platform, table) -> {
                if (!tableExists(table)) {
                    return;
                }
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
                              COALESCE(NULLIF(video_info.upload_platforms, ''), '') = ''
                              OR FIND_IN_SET(?, REPLACE(video_info.upload_platforms, ' ', '')) > 0
                          )
                        """.formatted(quotedIdentifier(table)), taskId, platform);
                repository.update("""
                        INSERT INTO uploader_task_status (task_id, platform, status)
                        VALUES (?, ?, 'ready')
                        ON DUPLICATE KEY UPDATE status = VALUES(status)
                        """, taskId, platform);
            });
        }
    }

    private void invalidateRouteLogs(String taskId, List<RouteNode> nodes) {
        if (!tableExists("distributor_route_log") || nodes.isEmpty()) return;
        String conditions = nodes.stream()
                .map(node -> "(from_stage = ? AND from_sub_stage = ?)")
                .reduce((left, right) -> left + " OR " + right)
                .orElseThrow();
        List<Object> arguments = new ArrayList<>();
        arguments.add(taskId);
        for (RouteNode node : nodes) {
            arguments.add(node.stage());
            arguments.add(node.subStage());
        }
        repository.update(
                "DELETE FROM distributor_route_log WHERE task_id = ? AND (" + conditions + ")",
                arguments.toArray()
        );
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
        for (StageRegistry.StagePolicy policy : stageRegistry.policies()) {
            if (!tableExists(policy.tableName())) {
                continue;
            }
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM " + quotedIdentifier(policy.tableName()) + " WHERE task_id = ? AND status = 'running'",
                    Integer.class,
                    taskId
            );
            if (count != null && count > 0) {
                return true;
            }
        }
        for (String childTable : List.of("translator_api_task", "speaker_segment", "asseter_jobs")) {
            if (!tableExists(childTable)) continue;
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM " + quotedIdentifier(childTable) + " WHERE task_id = ? AND status = 'running'",
                    Integer.class, taskId);
            if (count != null && count > 0) return true;
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
                  AND columns_info.TABLE_NAME <> 'operator_task'
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
        return;
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
            restoreVideoInfoMetadata(taskId);
            return;
        }
        repository.update("UPDATE video_info SET " + nullAssignments(resetColumns) + " WHERE task_id = ?", taskId);
        restoreVideoInfoSource(taskId);
        restoreVideoInfoMetadata(taskId);
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

    private void restoreVideoInfoMetadata(String taskId) {
        if (tableExists("downloader_submission")) {
            repository.update("""
                    UPDATE video_info video_info
                    JOIN downloader_submission submission ON submission.task_id = video_info.task_id
                    SET video_info.type = COALESCE(video_info.type, submission.type)
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
                    SET video_info.source_language = COALESCE(video_info.source_language, author.source_language),
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
