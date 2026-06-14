package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.FailureLogItem;
import com.youbi.monitor.model.FailureLogActualPublishedResult;
import com.youbi.monitor.repository.IFailureLogRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class FailureLogRepositoryServiceImpl extends MonitorRepositorySqlSupport implements IFailureLogRepositoryService {
    private static final Map<String, String> STAGE_TABLES = Map.of(
            "downloader", "downloader",
            "publisher", "publisher",
            "demucs", "demucs",
            "whisper", "whisper",
            "translator", "translator",
            "speaker", "speaker",
            "combiner", "combiner"
    );

    public FailureLogRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    @Override
    public List<FailureLogItem> listFailureLogs() {
        List<String> queries = new ArrayList<>();
        for (Map.Entry<String, String> entry : STAGE_TABLES.entrySet()) {
            queries.add(stageQuery(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : UPLOADER_TASK_TABLES.entrySet()) {
            queries.add(uploadQuery(entry.getKey(), entry.getValue()));
        }
        queries.add(genericUploaderQuery());

        String sql = """
                SELECT failure_log.*
                FROM (
                %s
                ) failure_log
                ORDER BY failure_log.failed_at DESC, failure_log.id DESC
                """.formatted(String.join("\nUNION ALL\n", queries));

        return repository.query(sql, (rs, rowNum) -> new FailureLogItem(
                rs.getString("id"),
                rs.getString("task_id"),
                rs.getString("title"),
                rs.getString("type"),
                rs.getString("stage"),
                rs.getString("platform"),
                rs.getString("account_key"),
                rs.getString("error_message"),
                timestamp(rs, "failed_at")
        ));
    }

    @Override
    @Transactional
    public FailureLogActualPublishedResult markActualPublished(String logId) {
        ActualPublishedTarget target = parseActualPublishedTarget(logId);
        String platform = normalizeUploadPlatform(target.platform());
        String table = UPLOADER_TASK_TABLES.get(platform);

        ActualPublishedSubmission submission = repository.queryForObject("""
                SELECT id, task_id, account_key, status
                FROM %s
                WHERE id = ?
                FOR UPDATE
                """.formatted(quotedIdentifier(table)), (rs, rowNum) -> new ActualPublishedSubmission(
                rs.getLong("id"),
                rs.getString("task_id"),
                rs.getString("account_key"),
                rs.getString("status")
        ), target.submissionId());
        if (!"failed".equals(submission.status())) {
            throw new IllegalStateException("上传子任务当前不是失败状态，无法标记实际发布。");
        }

        String manualResultSql = """
                JSON_OBJECT(
                  'mode', 'manual',
                  'actualPublished', TRUE,
                  'confirmedAt', DATE_FORMAT(NOW(), '%%Y-%%m-%%dT%%H:%%i:%%s'),
                  'response', JSON_OBJECT(
                    'success', TRUE,
                    'status', 'success',
                    'message', '人工确认实际发布',
                    'platform', ?,
                    'taskId', ?,
                    'accountKey', ?,
                    'accountName', ?
                  )
                )
                """;
        int submissionUpdated = repository.update("""
                UPDATE %s
                SET status = 'success',
                    result_json = %s,
                    error_message = NULL,
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NOW()
                WHERE id = ? AND status = 'failed'
                """.formatted(quotedIdentifier(table), manualResultSql),
                platform,
                submission.taskId(),
                submission.accountKey(),
                submission.accountKey(),
                submission.id()
        );
        if (submissionUpdated != 1) {
            throw new IllegalStateException("上传子任务状态已变化，请刷新后重试。");
        }

        applyUploaderAccountStatusChanges(platform, List.of(new UploadAccountStatusChange(
                submission.taskId(),
                submission.accountKey(),
                "failed",
                "success"
        )));
        updateUploaderPlatformResult(platform, submission, manualResultSql);
        markLatestMonitorUploadActualPublished(platform, submission);

        ParentStatuses parentStatuses = updateParentStatuses(submission.taskId());
        return new FailureLogActualPublishedResult(
                submission.taskId(),
                platform,
                submission.id(),
                "success",
                parentStatuses.uploaderStatus(),
                parentStatuses.taskStatus()
        );
    }

    private void updateUploaderPlatformResult(
            String platform,
            ActualPublishedSubmission submission,
            String manualResultSql
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder assignments = new StringBuilder(quotedIdentifier(uploadStatusColumn(platform)) + " = 'success'");
        String resultColumn = platformResultColumn(platform);
        if (!resultColumn.isBlank()) {
            assignments.append(", ").append(quotedIdentifier(resultColumn)).append(" = ").append(manualResultSql);
            args.add(platform);
            args.add(submission.taskId());
            args.add(submission.accountKey());
            args.add(submission.accountKey());
        }
        if ("bilibili".equals(platform)) {
            assignments.append(", biliup = NULL, playwright = 'success'");
        }
        String accountKeyColumn = platformAccountKeyColumn(platform);
        if (!accountKeyColumn.isBlank()) {
            assignments.append(", ").append(quotedIdentifier(accountKeyColumn)).append(" = ?");
            args.add(submission.accountKey());
        }
        args.add(submission.taskId());
        int updated = repository.update("""
                UPDATE uploader
                SET %s
                WHERE task_id = ?
                """.formatted(assignments), args.toArray());
        if (updated != 1) {
            throw new IllegalStateException("Uploader 父任务不存在。");
        }
    }

    private void markLatestMonitorUploadActualPublished(String platform, ActualPublishedSubmission submission) {
        if (!tableExists("monitor_upload_task")) {
            return;
        }
        repository.update("""
                UPDATE monitor_upload_task
                SET status = 'success',
                    result_json = JSON_OBJECT(
                      'durationMs', TIMESTAMPDIFF(MICROSECOND, started_at, NOW()) DIV 1000,
                      'manualActualPublished', TRUE,
                      'confirmedAt', DATE_FORMAT(NOW(), '%%Y-%%m-%%dT%%H:%%i:%%s'),
                      'result', JSON_OBJECT(
                        'success', TRUE,
                        'accountKey', ?,
                        'accountName', ?,
                        'message', '人工确认实际发布',
                        'raw', JSON_OBJECT('manualActualPublished', TRUE)
                      )
                    ),
                    error_code = NULL,
                    error_message = NULL,
                    completed_at = NOW()
                WHERE platform = ?
                  AND upstream_task_id = ?
                  AND account_key = ?
                  AND status <> 'success'
                ORDER BY id DESC
                LIMIT 1
                """,
                platform,
                submission.taskId(),
                submission.accountKey(),
                submission.accountKey(),
                submission.accountKey()
        );
    }

    private ParentStatuses updateParentStatuses(String taskId) {
        UploaderPlatformStatuses statuses = repository.queryForObject("""
                SELECT
                  COALESCE(NULLIF(bilibili_upload_status, ''), 'no_need') bilibili,
                  COALESCE(NULLIF(douyin_upload_status, ''), 'no_need') douyin,
                  COALESCE(NULLIF(xiaohongshu_upload_status, ''), 'no_need') xiaohongshu,
                  COALESCE(NULLIF(shipinhao_upload_status, ''), 'no_need') shipinhao,
                  COALESCE(NULLIF(kuaishou_upload_status, ''), 'no_need') kuaishou,
                  COALESCE(NULLIF(jinritoutiao_upload_status, ''), 'no_need') jinritoutiao
                FROM uploader
                WHERE task_id = ?
                FOR UPDATE
                """, (rs, rowNum) -> new UploaderPlatformStatuses(List.of(
                rs.getString("bilibili"),
                rs.getString("douyin"),
                rs.getString("xiaohongshu"),
                rs.getString("shipinhao"),
                rs.getString("kuaishou"),
                rs.getString("jinritoutiao")
        )), taskId);

        boolean hasFailed = statuses.values().stream().anyMatch("failed"::equals);
        boolean complete = statuses.values().stream().allMatch(status -> "success".equals(status) || "no_need".equals(status));
        String uploaderStatus = hasFailed ? "failed" : complete ? "success" : "running";
        String taskStatus = uploaderStatus;
        String currentStage = complete ? "done" : "uploader";
        String error = hasFailed ? "one or more upload submissions failed" : null;

        repository.update("""
                UPDATE uploader
                SET status = ?,
                    completed_at = CASE WHEN ? = 'running' THEN NULL ELSE NOW() END,
                    error_message = ?,
                    `operator` = CASE WHEN ? = 'running' THEN NULL ELSE `operator` END
                WHERE task_id = ?
                """, uploaderStatus, uploaderStatus, error, uploaderStatus, taskId);
        repository.update("""
                UPDATE task
                SET status = ?,
                    current_stage = ?,
                    completed_at = CASE WHEN ? = 'running' THEN NULL ELSE NOW() END,
                    error_message = ?
                WHERE id = ?
                """, taskStatus, currentStage, taskStatus, error, taskId);
        return new ParentStatuses(uploaderStatus, taskStatus);
    }

    private static ActualPublishedTarget parseActualPublishedTarget(String logId) {
        String normalized = text(logId);
        String[] parts = normalized.split(":", 3);
        if (parts.length != 3 || !"uploader".equals(parts[0])) {
            throw new IllegalArgumentException("只有 Uploader 平台失败日志支持标记实际发布。");
        }
        try {
            long submissionId = Long.parseLong(parts[2]);
            if (submissionId <= 0) {
                throw new NumberFormatException();
            }
            return new ActualPublishedTarget(parts[1], submissionId);
        } catch (NumberFormatException exc) {
            throw new IllegalArgumentException("无效的上传子任务 ID。", exc);
        }
    }

    private static String platformResultColumn(String platform) {
        return switch (platform) {
            case "bilibili" -> "upload_result_json";
            case "douyin" -> "douyin_upload_result_json";
            case "xiaohongshu" -> "xiaohongshu_upload_result_json";
            case "shipinhao" -> "shipinhao_upload_result_json";
            case "kuaishou" -> "kuaishou_upload_result_json";
            case "jinritoutiao" -> "jinritoutiao_upload_result_json";
            default -> "";
        };
    }

    private static String platformAccountKeyColumn(String platform) {
        return switch (platform) {
            case "douyin" -> "douyin_upload_account_key";
            case "xiaohongshu" -> "xiaohongshu_upload_account_key";
            case "shipinhao" -> "shipinhao_upload_account_key";
            case "kuaishou" -> "kuaishou_upload_account_key";
            case "jinritoutiao" -> "jinritoutiao_upload_account_key";
            default -> "";
        };
    }

    private record ActualPublishedTarget(String platform, long submissionId) {
    }

    private record ActualPublishedSubmission(long id, String taskId, String accountKey, String status) {
    }

    private record UploaderPlatformStatuses(List<String> values) {
    }

    private record ParentStatuses(String uploaderStatus, String taskStatus) {
    }

    private static String stageQuery(String stage, String table) {
        return """
                SELECT
                  CONCAT('%s:', stage.task_id) id,
                  stage.task_id,
                  COALESCE(NULLIF(video_info.upload_title, ''), NULLIF(source_video.title, ''), stage.task_id) title,
                  COALESCE(NULLIF(video_info.type, ''), '未分类') type,
                  '%s' stage,
                  '' platform,
                  '' account_key,
                  COALESCE(NULLIF(stage.error_message, ''), NULLIF(task.error_message, ''), '未知错误') error_message,
                  COALESCE(stage.completed_at, stage.started_at, task.completed_at, task.started_at, task.created_at) failed_at
                FROM %s stage
                JOIN task task ON task.id = stage.task_id
                LEFT JOIN video_info video_info ON video_info.task_id = stage.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                WHERE stage.status = 'failed'
                """.formatted(stage, stage, quotedIdentifier(table));
    }

    private static String uploadQuery(String platform, String table) {
        return """
                SELECT
                  CONCAT('uploader:%s:', submission.id) id,
                  submission.task_id,
                  COALESCE(NULLIF(video_info.upload_title, ''), NULLIF(source_video.title, ''), submission.task_id) title,
                  COALESCE(NULLIF(video_info.type, ''), '未分类') type,
                  'uploader' stage,
                  '%s' platform,
                  COALESCE(submission.account_key, '') account_key,
                  COALESCE(NULLIF(submission.error_message, ''), NULLIF(uploader.error_message, ''), NULLIF(task.error_message, ''), '未知错误') error_message,
                  COALESCE(submission.completed_at, submission.updated_at, submission.started_at, submission.created_at) failed_at
                FROM %s submission
                JOIN task task ON task.id = submission.task_id
                LEFT JOIN uploader uploader ON uploader.task_id = submission.task_id
                LEFT JOIN video_info video_info ON video_info.task_id = submission.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                WHERE submission.status = 'failed'
                """.formatted(platform, platform, quotedIdentifier(table));
    }

    private static String genericUploaderQuery() {
        String failedUploadExists = UPLOADER_TASK_TABLES.values().stream()
                .map(table -> """
                        SELECT 1 FROM %s submission
                        WHERE submission.task_id = uploader.task_id AND submission.status = 'failed'
                        """.formatted(quotedIdentifier(table)))
                .map(query -> "EXISTS (" + query + ")")
                .reduce((left, right) -> left + " OR " + right)
                .orElse("FALSE");
        return """
                SELECT
                  CONCAT('uploader:', uploader.task_id) id,
                  uploader.task_id,
                  COALESCE(NULLIF(video_info.upload_title, ''), NULLIF(source_video.title, ''), uploader.task_id) title,
                  COALESCE(NULLIF(video_info.type, ''), '未分类') type,
                  'uploader' stage,
                  '' platform,
                  '' account_key,
                  COALESCE(NULLIF(uploader.error_message, ''), NULLIF(task.error_message, ''), '未知错误') error_message,
                  COALESCE(uploader.completed_at, uploader.started_at, task.completed_at, task.started_at, task.created_at) failed_at
                FROM uploader uploader
                JOIN task task ON task.id = uploader.task_id
                LEFT JOIN video_info video_info ON video_info.task_id = uploader.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                WHERE uploader.status = 'failed'
                  AND NOT (%s)
                """.formatted(failedUploadExists);
    }
}
