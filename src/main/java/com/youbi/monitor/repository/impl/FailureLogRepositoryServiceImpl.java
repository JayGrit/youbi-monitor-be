package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.FailureLogItem;
import com.youbi.monitor.repository.IFailureLogRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
public class FailureLogRepositoryServiceImpl extends MonitorRepositorySqlSupport implements IFailureLogRepositoryService {
    public FailureLogRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    @Override
    public List<FailureLogItem> listFailureLogs() {
        List<String> queries = new ArrayList<>();
        Set<String> uploadTaskTables = new LinkedHashSet<>();
        for (String table : new LinkedHashSet<>(UPLOADER_TASK_TABLES.values())) {
            if (!tableExists(table)) {
                continue;
            }
            uploadTaskTables.add(table);
            queries.add(uploadQuery(table));
        }
        queries.add(distributorStageQuery(uploadTaskTables));

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
                rs.getString("topic"),
                rs.getString("stage"),
                rs.getString("sub_stage"),
                rs.getString("platform"),
                rs.getString("error_message"),
                timestamp(rs, "failed_at")
        ));
    }

    private static String uploadQuery(String table) {
        return """
                SELECT
                  CONCAT('uploader:', submission.platform, ':', submission.id) id,
                  submission.task_id,
                  COALESCE(NULLIF(meta.upload_title, ''), NULLIF(source.source_title, ''), submission.task_id) title,
                  COALESCE(NULLIF(task.topic, ''), '未分类') topic,
                  'uploader' stage,
                  'main' sub_stage,
                  submission.platform platform,
                  COALESCE(NULLIF(submission.error_message, ''), NULLIF(uploader.error_message, ''), NULLIF(task.error_message, ''), '未知错误') error_message,
                  COALESCE(submission.completed_at, submission.updated_at, submission.started_at, submission.created_at) failed_at
                FROM %s submission
                JOIN task task ON task.id = submission.task_id
                LEFT JOIN distributor_task_stages uploader
                  ON uploader.task_id = submission.task_id
                 AND uploader.stage_name = 'uploader'
                 AND uploader.sub_stage = 'main'
                LEFT JOIN task_metadata meta ON meta.task_id = submission.task_id
                LEFT JOIN task_source source ON source.task_id = submission.task_id
                WHERE submission.status = 'failed'
                """.formatted(quotedIdentifier(table));
    }

    private static String distributorStageQuery(Set<String> uploadTaskTables) {
        String failedUploadExists = uploadTaskTables.stream()
                .map(table -> """
                        SELECT 1 FROM %s submission
                        WHERE submission.task_id = stage.task_id AND submission.status = 'failed'
                        """.formatted(quotedIdentifier(table)))
                .map(query -> "EXISTS (" + query + ")")
                .reduce((left, right) -> left + " OR " + right)
                .orElse("FALSE");
        return """
                SELECT
                  CONCAT('stage:', stage.stage_name, ':', stage.sub_stage, ':', stage.task_id) id,
                  stage.task_id,
                  COALESCE(NULLIF(meta.upload_title, ''), NULLIF(source.source_title, ''), stage.task_id) title,
                  COALESCE(NULLIF(task.topic, ''), '未分类') topic,
                  stage.stage_name stage,
                  stage.sub_stage sub_stage,
                  '' platform,
                  COALESCE(NULLIF(stage.error_message, ''), NULLIF(task.error_message, ''), '未知错误') error_message,
                  COALESCE(stage.completed_at, stage.updated_at, stage.started_at, task.completed_at, task.started_at, task.created_at) failed_at
                FROM distributor_task_stages stage
                JOIN task task ON task.id = stage.task_id
                LEFT JOIN task_metadata meta ON meta.task_id = stage.task_id
                LEFT JOIN task_source source ON source.task_id = stage.task_id
                WHERE stage.status = 'failed'
                  AND NOT (stage.stage_name = 'uploader' AND stage.sub_stage = 'main' AND (%s))
                """.formatted(failedUploadExists);
    }
}
