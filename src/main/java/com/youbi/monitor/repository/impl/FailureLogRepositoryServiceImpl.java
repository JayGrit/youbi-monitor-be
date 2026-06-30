package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.FailureLogItem;
import com.youbi.monitor.repository.IFailureLogRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

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
            if (!tableExists(entry.getValue())) {
                continue;
            }
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
                LEFT JOIN distributor_task_stages uploader
                  ON uploader.task_id = submission.task_id
                 AND uploader.stage_name = 'uploader'
                 AND uploader.sub_stage = 'main'
                LEFT JOIN video_info video_info ON video_info.task_id = submission.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                WHERE submission.platform = '%s'
                  AND submission.status = 'failed'
                """.formatted(platform, platform, quotedIdentifier(table), platform);
    }

    private String genericUploaderQuery() {
        String failedUploadExists = UPLOADER_TASK_TABLES.values().stream()
                .filter(this::tableExists)
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
                FROM distributor_task_stages uploader
                JOIN task task ON task.id = uploader.task_id
                LEFT JOIN video_info video_info ON video_info.task_id = uploader.task_id
                LEFT JOIN submitter_video source_video ON source_video.id = video_info.submitter_video_id
                WHERE uploader.stage_name = 'uploader'
                  AND uploader.sub_stage = 'main'
                  AND uploader.status = 'failed'
                  AND NOT (%s)
                """.formatted(failedUploadExists);
    }
}
