package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.IUploadSubmissionRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.service.MonitorService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
public class UploadSubmissionRepositoryServiceImpl extends MonitorRepositorySqlSupport implements IUploadSubmissionRepositoryService {
    public UploadSubmissionRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    
    public MonitorService.FailedUploadSubmissionList listFailedUploadSubmissions(String platform) {
        String normalized = normalizeUploadPlatform(platform);
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        if (!tableExists(table)) {
            return new MonitorService.FailedUploadSubmissionList(normalized, 0, List.of());
        }
        boolean accountTableExists = tableExists(accountTable);
        String submissionTitleSql = submissionTitleSql(table);
        String accountJoin = accountTableExists
                ? "LEFT JOIN " + quotedIdentifier(accountTable) + " account ON account.account_key = submission.account_key"
                : "";
        String accountExistsSql = accountTableExists ? "account.account_key IS NOT NULL" : "FALSE";
        List<MonitorService.FailedUploadSubmission> rows = repository.query("""
                SELECT
                  submission.id,
                  submission.task_id,
                  %s title,
                  submission.account_key,
                  submission.error_message,
                  submission.completed_at,
                  submission.updated_at,
                  task.status task_status,
                  uploader.status uploader_status,
                  COALESCE(NULLIF(uploader.upload_platforms, ''), '') upload_platforms,
                  video_info.type routed_account_key,
                  %s account_exists
                FROM %s submission
                JOIN task task ON task.id = submission.task_id
                JOIN uploader uploader ON uploader.task_id = submission.task_id
                LEFT JOIN video_info video_info ON video_info.task_id = submission.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                %s
                WHERE submission.status = 'failed'
                ORDER BY submission.updated_at DESC, submission.id DESC
                LIMIT 200
                """.formatted(submissionTitleSql, accountExistsSql, quotedIdentifier(table), accountJoin), (rs, rowNum) -> {
            boolean accountExists = rs.getBoolean("account_exists");
            String retryBlockedReason = accountExists ? "" : "账号表中不存在该 account_key，worker 不会拉取";
            return new MonitorService.FailedUploadSubmission(
                    rs.getLong("id"),
                    normalized,
                    rs.getString("task_id"),
                    rs.getString("title"),
                    rs.getString("account_key"),
                    rs.getString("error_message"),
                    timestamp(rs, "completed_at"),
                    timestamp(rs, "updated_at"),
                    rs.getString("task_status"),
                    rs.getString("uploader_status"),
                    rs.getString("upload_platforms"),
                    rs.getString("routed_account_key"),
                    accountExists,
                    retryBlockedReason
            );
        });
        return new MonitorService.FailedUploadSubmissionList(normalized, rows.size(), rows);
    }

    private String submissionTitleSql(String table) {
        return "COALESCE("
                + nullableTextColumnSql(table, "submission", "title")
                + ", "
                + nullableTextColumnSql("video_info", "video_info", "upload_title")
                + ", "
                + nullableTextColumnSql("submitter_video", "source_video", "title")
                + ", submission.task_id)";
    }

    private String nullableTextColumnSql(String table, String alias, String column) {
        return columnExists(table, column) ? "NULLIF(" + alias + "." + column + ", '')" : "NULL";
    }

    @Transactional
    
    public MonitorService.UploadSubmissionRetryResult retryFailedUploadSubmissions(String platform, List<Long> ids) {
        String normalized = normalizeUploadPlatform(platform);
        List<Long> normalizedIds = ids == null ? List.of() : ids.stream()
                .filter(id -> id != null && id > 0)
                .distinct()
                .toList();
        if (normalizedIds.isEmpty()) {
            throw new IllegalArgumentException("No upload submission selected.");
        }
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        if (!tableExists(table)) {
            throw new IllegalArgumentException("Upload task table does not exist for platform: " + normalized);
        }
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }

        String placeholders = placeholders(normalizedIds.size());
        List<UploadAccountStatusChange> accountStatusChanges = repository.query("""
                SELECT submission.task_id, submission.account_key, submission.status
                FROM %s submission
                JOIN %s account ON account.account_key = submission.account_key
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                FOR UPDATE
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders),
                (rs, rowNum) -> new UploadAccountStatusChange(
                        rs.getString("task_id"),
                        rs.getString("account_key"),
                        rs.getString("status"),
                        "ready"
                ),
                normalizedIds.toArray()
        );
        if (accountStatusChanges.isEmpty()) {
            return new MonitorService.UploadSubmissionRetryResult(normalized, 0, 0, 0);
        }

        List<String> taskIds = repository.queryForList("""
                SELECT DISTINCT submission.task_id
                FROM %s submission
                JOIN %s account ON account.account_key = submission.account_key
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders), String.class, normalizedIds.toArray());
        if (taskIds.isEmpty()) {
            return new MonitorService.UploadSubmissionRetryResult(normalized, 0, 0, 0);
        }

        int retried = repository.update("""
                UPDATE %s submission
                JOIN %s account ON account.account_key = submission.account_key
                SET submission.status = 'ready',
                    submission.started_at = NULL,
                    submission.completed_at = NULL,
                    submission.error_message = NULL,
                    submission.operator_upload_status = NULL,
                    submission.monitor_be_upload_status = NULL,
                    submission.operator_op_id = NULL,
                    submission.operator_run_id = NULL,
                    submission.operator_task_id = NULL,
                    submission.operator_next_check_at = NULL,
                    submission.operator_deadline_at = NULL
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders), normalizedIds.toArray());
        if (retried > 0) {
            applyUploaderAccountStatusChanges(normalized, accountStatusChanges);
        }

        String taskPlaceholders = placeholders(taskIds.size());
        Object[] taskArgs = taskIds.toArray();
        int uploaderUpdated = repository.update("""
                UPDATE uploader
                SET status = 'running',
                    %s = 'ready',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id IN (%s)
                """.formatted(quotedIdentifier(uploadStatusColumn(normalized)), taskPlaceholders), taskArgs);
        int taskUpdated = repository.update("""
                UPDATE task
                SET status = 'running',
                    current_stage = 'uploader',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL
                WHERE id IN (%s)
                """.formatted(taskPlaceholders), taskArgs);
        return new MonitorService.UploadSubmissionRetryResult(normalized, retried, uploaderUpdated, taskUpdated);
    }

    
    public MonitorService.UploadBackfillCandidateList listUploadBackfillCandidates(String platform, String accountKey, String type) {
        String normalized = normalizeUploadPlatform(platform);
        String normalizedAccountKey = text(accountKey);
        String normalizedType = text(type);
        if (normalizedAccountKey.isBlank()) {
            throw new IllegalArgumentException("Missing accountKey.");
        }
        if (normalizedType.isBlank()) {
            throw new IllegalArgumentException("Missing type.");
        }
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        if (!tableExists(table)) {
            return new MonitorService.UploadBackfillCandidateList(normalized, normalizedAccountKey, normalizedType, 0, List.of());
        }
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }
        String candidateTitleSql = "COALESCE("
                + nullableTextColumnSql("video_info", "video_info", "upload_title")
                + ", "
                + nullableTextColumnSql("submitter_video", "source_video", "title")
                + ", task.id)";
        String candidateCoverSql = "COALESCE("
                + nullableTextColumnSql("video_info", "video_info", "final_cover_url")
                + ", "
                + nullableTextColumnSql("video_info", "video_info", "clean_cover_url")
                + ", "
                + nullableTextColumnSql("video_info", "video_info", "source_cover_url")
                + ", "
                + nullableTextColumnSql("video_info", "video_info", "source_thumbnail_url")
                + ", NULL)";

        List<MonitorService.UploadBackfillCandidate> rows = repository.query("""
                SELECT
                  task.id task_id,
                  %s title,
                  %s cover_url,
                  %s final_video_ref,
                  COALESCE(uploader.completed_at, task.completed_at, task.created_at) completed_at,
                  GROUP_CONCAT(DISTINCT sent.platform ORDER BY sent.platform SEPARATOR ',') uploaded_platforms,
                  target.id target_submission_id,
                  target.status target_status,
                  platform_account.account_key IS NOT NULL account_exists,
                  COALESCE(account.is_enabled, 0) account_enabled,
                  COALESCE(account.is_available, 0) account_available
                FROM task task
                JOIN video_info video_info ON video_info.task_id = task.id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                JOIN uploader uploader ON uploader.task_id = task.id
                JOIN (
                  %s
                ) sent ON sent.task_id = task.id
                LEFT JOIN %s target ON target.task_id = task.id AND target.account_key = ?
                LEFT JOIN %s platform_account ON platform_account.account_key = ?
                LEFT JOIN %s account ON account.platform = ? AND account.account_key = ? AND account.is_deprecated = 0
                WHERE video_info.type = ?
                GROUP BY
                  task.id,
                  title,
                  cover_url,
                  final_video_ref,
                  completed_at,
                  target.id,
                  target.status,
                  account_exists,
                  account_enabled,
                  account_available
                ORDER BY completed_at DESC
                LIMIT 500
                """.formatted(candidateTitleSql, candidateCoverSql, finalVideoRefSql(), successfulUploadUnion(normalized), quotedIdentifier(table), quotedIdentifier(accountTable), UNIFIED_UPLOADER_ACCOUNT_TABLE),
                (rs, rowNum) -> {
                    boolean accountExists = rs.getBoolean("account_exists");
                    boolean accountEnabled = rs.getBoolean("account_enabled");
                    boolean accountAvailable = rs.getBoolean("account_available");
                    String finalVideoRef = rs.getString("final_video_ref");
                    long targetSubmissionId = rs.getLong("target_submission_id");
                    boolean hasTargetSubmission = !rs.wasNull();
                    String blockedReason = "";
                    if (!accountExists) {
                        blockedReason = "目标账号不存在";
                    } else if (!accountEnabled) {
                        blockedReason = "目标账号已禁用";
                    } else if (!accountAvailable) {
                        blockedReason = "目标账号登录态不可用";
                    } else if (text(finalVideoRef).isBlank()) {
                        blockedReason = "缺少 final_video_url";
                    } else if (hasTargetSubmission) {
                        blockedReason = "目标账号已存在发送任务：" + text(rs.getString("target_status"));
                    }
                    return new MonitorService.UploadBackfillCandidate(
                            rs.getString("task_id"),
                            rs.getString("title"),
                            rs.getString("cover_url"),
                            finalVideoRef,
                            splitCsv(rs.getString("uploaded_platforms")),
                            timestamp(rs, "completed_at"),
                            blockedReason.isBlank(),
                            blockedReason
                    );
                },
                normalizedAccountKey,
                normalizedAccountKey,
                normalized,
                normalizedAccountKey,
                normalizedType
        );
        return new MonitorService.UploadBackfillCandidateList(normalized, normalizedAccountKey, normalizedType, rows.size(), rows);
    }

    @Transactional
    
    public MonitorService.UploadBackfillRegisterResult registerUploadBackfill(String platform, String accountKey, String type, List<String> taskIds) {
        String normalized = normalizeUploadPlatform(platform);
        String normalizedAccountKey = text(accountKey);
        String normalizedType = text(type);
        if (normalizedAccountKey.isBlank()) {
            throw new IllegalArgumentException("Missing accountKey.");
        }
        if (normalizedType.isBlank()) {
            throw new IllegalArgumentException("Missing type.");
        }
        List<String> normalizedTaskIds = taskIds == null ? List.of() : taskIds.stream()
                .map(MonitorRepositorySqlSupport::text)
                .filter(value -> !value.isBlank())
                .distinct()
                .toList();
        if (normalizedTaskIds.isEmpty()) {
            throw new IllegalArgumentException("No task selected.");
        }
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        if (!tableExists(table)) {
            throw new IllegalArgumentException("Upload task table does not exist for platform: " + normalized);
        }
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }

        String taskPlaceholders = placeholders(normalizedTaskIds.size());
        Object[] queryArgs = new Object[3 + normalizedTaskIds.size()];
        queryArgs[0] = normalizedAccountKey;
        queryArgs[1] = normalized;
        for (int i = 0; i < normalizedTaskIds.size(); i++) {
            queryArgs[i + 2] = normalizedTaskIds.get(i);
        }
        queryArgs[queryArgs.length - 1] = normalizedType;

        List<UploadBackfillInsertRow> rows = repository.query("""
                SELECT
                  task.id task_id
                FROM task task
                JOIN video_info video_info ON video_info.task_id = task.id
                JOIN uploader uploader ON uploader.task_id = task.id
                JOIN %s platform_account ON platform_account.account_key = ?
                JOIN %s account ON account.platform = ?
                    AND account.account_key = platform_account.account_key
                    AND account.is_deprecated = 0
                    AND account.is_enabled = 1
                    AND account.is_available = 1
                JOIN (
                  %s
                ) sent ON sent.task_id = task.id
                LEFT JOIN %s target ON target.task_id = task.id AND target.account_key = account.account_key
                WHERE task.id IN (%s)
                  AND video_info.type = ?
                  AND COALESCE(NULLIF(%s, ''), '') <> ''
                  AND target.id IS NULL
                GROUP BY
                  task.id
                """.formatted(
                        quotedIdentifier(accountTable),
                        UNIFIED_UPLOADER_ACCOUNT_TABLE,
                        successfulUploadUnion(normalized),
                        quotedIdentifier(table),
                        taskPlaceholders,
                        finalVideoRefSql()
                ),
                (rs, rowNum) -> new UploadBackfillInsertRow(rs.getString("task_id")),
                queryArgs
        );
        if (rows.isEmpty()) {
            return new MonitorService.UploadBackfillRegisterResult(normalized, normalizedAccountKey, normalizedType, 0, normalizedTaskIds.size(), 0, 0);
        }

        int registered = 0;
        List<UploadAccountStatusChange> accountStatusChanges = new ArrayList<>();
        for (UploadBackfillInsertRow row : rows) {
            int inserted = repository.update("""
                    INSERT INTO %s (
                        task_id, account_key, status
                    )
                    VALUES (?, ?, 'ready')
                    """.formatted(quotedIdentifier(table)),
                    row.taskId(),
                    normalizedAccountKey
            );
            registered += inserted;
            if (inserted > 0) {
                accountStatusChanges.add(new UploadAccountStatusChange(row.taskId(), normalizedAccountKey, null, "ready"));
            }
        }
        applyUploaderAccountStatusChanges(normalized, accountStatusChanges);

        List<String> registeredTaskIds = rows.stream().map(UploadBackfillInsertRow::taskId).distinct().toList();
        String registeredPlaceholders = placeholders(registeredTaskIds.size());
        List<Object> uploaderArgs = new ArrayList<>();
        uploaderArgs.add(normalized);
        uploaderArgs.add(normalized);
        uploaderArgs.add(normalized);
        uploaderArgs.addAll(registeredTaskIds);
        int uploaderUpdated = repository.update("""
                UPDATE uploader
                SET status = 'running',
                    upload_platforms = CASE
                        WHEN COALESCE(NULLIF(TRIM(upload_platforms), ''), '') = '' THEN ?
                        WHEN FIND_IN_SET(?, REPLACE(upload_platforms, ' ', '')) > 0 THEN upload_platforms
                        ELSE CONCAT(upload_platforms, ',', ?)
                    END,
                    %s = 'ready',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id IN (%s)
                """.formatted(quotedIdentifier(uploadStatusColumn(normalized)), registeredPlaceholders), uploaderArgs.toArray());
        Object[] registeredArgs = registeredTaskIds.toArray();
        int taskUpdated = repository.update("""
                UPDATE task
                SET status = 'running',
                    current_stage = 'uploader',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL
                WHERE id IN (%s)
                """.formatted(registeredPlaceholders), registeredArgs);
        return new MonitorService.UploadBackfillRegisterResult(
                normalized,
                normalizedAccountKey,
                normalizedType,
                registered,
                Math.max(0, normalizedTaskIds.size() - registeredTaskIds.size()),
                uploaderUpdated,
                taskUpdated
        );
    }


}
