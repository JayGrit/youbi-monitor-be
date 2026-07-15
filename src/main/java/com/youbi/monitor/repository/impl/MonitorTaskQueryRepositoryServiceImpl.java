package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.DeviceHeartbeat;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.model.StageError;
import com.youbi.monitor.model.TaskMonitorItem;
import com.youbi.monitor.model.TaskProgressDetail;
import com.youbi.monitor.model.UploadPlatformStatus;
import com.youbi.monitor.repository.IMonitorTaskQueryRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MonitorTaskQueryRepositoryServiceImpl extends MonitorRepositorySqlSupport implements IMonitorTaskQueryRepositoryService {
    private final Map<String, List<String>> tableColumnsCache = new ConcurrentHashMap<>();
    private final TaskProgressRouteGraphBuilder routeGraphBuilder;
    private static final long HEARTBEAT_ONLINE_SECONDS = 60;
    private static final List<String> HEARTBEAT_DEVICES = List.of(
            "Macbook Air M4",
            "Macmini M2",
            "LPXB",
            "MY_HP",
            "LPXB_HP",
            "TXY"
    );
    private static final String MONITOR_SQL = """
            SELECT
              t.id,
              COALESCE(NULLIF(vi.upload_title, ''), NULLIF(sv.title, ''), t.id) title,
              vi.source_url,
              sv.webpage_url source_webpage_url,
              vi.source_thumbnail_url,
              sv.duration source_duration_seconds,
              bm.minio_storage_bytes,
              bm.minio_storage_object_count,
              bm.minio_storage_updated_at,
              vi.type task_type,
              vi.task_type distributor_task_type,
              vi.has_background_audio,
              __DISTRIBUTOR_STAGES_SELECT__
              t.status,
              t.current_stage,
              t.created_at,
              t.started_at,
              t.completed_at,
              t.error_message,

              d.status downloader_status,
              d.started_at downloader_started_at,
              d.completed_at downloader_completed_at,
              d.error_message downloader_error,
              __DOWNLOADER_PROGRESS_SELECT__

              de.status demucs_status,
              de.started_at demucs_started_at,
              de.completed_at demucs_completed_at,
              de.error_message demucs_error,

              w.status whisper_status,
              w.started_at whisper_started_at,
              w.completed_at whisper_completed_at,
              w.error_message whisper_error,

              CASE WHEN COALESCE(tf.translator_failed_count, 0) > 0 THEN 'failed' ELSE tr.status END translator_status,
              tr.started_at translator_started_at,
              tr.completed_at translator_completed_at,
              tr.error_message translator_error,
              COALESCE(tc.translator_completed_count, ts.translated_count) translator_completed_count,
              tf.translator_failed_count translator_failed_count,
              COALESCE(tc.translator_total_count, GREATEST(COALESCE(fa.fixed_count, 0), COALESCE(ts.translated_count, 0))) translator_total_count,
              NULL translator_child_error,

              CASE WHEN COALESCE(ss.speaker_failed_count, 0) > 0 THEN 'failed' ELSE sp.status END speaker_status,
              sp.started_at speaker_started_at,
              sp.completed_at speaker_completed_at,
              sp.error_message speaker_error,
              ss.speaker_completed_count,
              ss.speaker_failed_count,
              ss.speaker_total_count,
              NULL speaker_child_error,

              __ASSETER_SELECT__

              m.status combiner_status,
              m.started_at combiner_started_at,
              m.completed_at combiner_completed_at,
              m.error_message combiner_error,

              pub.status publisher_status,
              pub.started_at publisher_started_at,
              pub.completed_at publisher_completed_at,
              COALESCE(NULLIF(pub.error_message, ''), pr.error_message) publisher_error,

              CASE WHEN COALESCE(us.uploader_failed_count, 0) > 0 THEN 'failed' ELSE u.status END uploader_status,
              u.started_at uploader_started_at,
              u.completed_at uploader_completed_at,
              COALESCE(us.uploader_completed_count, 0) uploader_completed_count,
              COALESCE(us.uploader_failed_count, 0) uploader_failed_count,
              COALESCE(us.uploader_total_count, 0) uploader_total_count,
              us.bilibili_upload_status,
              us.douyin_upload_status,
              us.xiaohongshu_upload_status,
              us.shipinhao_upload_status,
              us.kuaishou_upload_status,
              us.jinritoutiao_upload_status,
              us.youtube_upload_status,
              us.x_upload_status,
              (
                SELECT submission.account_key
                FROM uploader_task submission
                WHERE submission.task_id = t.id
                  AND submission.platform = 'bilibili'
                ORDER BY FIELD(submission.status, 'success', 'running', 'ready', 'failed'), submission.id DESC
                LIMIT 1
              ) bilibili_upload_account_key,
              (
                SELECT COALESCE(phone_profile.display_name, loginstate.account_key, submission.account_key)
                FROM uploader_task submission
                LEFT JOIN operator_loginstate loginstate
                  ON loginstate.platform = submission.platform
                 AND loginstate.account_key = submission.account_key
                LEFT JOIN (
                  SELECT platform, account_id, MAX(display_name) AS display_name
                  FROM uploader_phone_account
                  GROUP BY platform, account_id
                ) phone_profile
                  ON phone_profile.platform = loginstate.platform
                 AND phone_profile.account_id = loginstate.id
                WHERE submission.task_id = t.id
                  AND submission.platform = 'bilibili'
                ORDER BY FIELD(submission.status, 'success', 'running', 'ready', 'failed'), submission.id DESC
                LIMIT 1
              ) bilibili_upload_account_name,
              u.error_message uploader_error
            FROM task t
            LEFT JOIN distributor_task_stages d ON d.task_id = t.id AND d.stage_name = 'downloader' AND d.sub_stage = 'main'
            LEFT JOIN distributor_task_stages de ON de.task_id = t.id AND de.stage_name = 'demucs' AND de.sub_stage = 'main'
            LEFT JOIN distributor_task_stages w ON w.task_id = t.id AND w.stage_name = 'whisper' AND w.sub_stage = 'main'
            LEFT JOIN distributor_task_stages tr ON tr.task_id = t.id AND tr.stage_name = 'translator' AND tr.sub_stage = 'main'
            LEFT JOIN distributor_task_stages sp ON sp.task_id = t.id AND sp.stage_name = 'speaker' AND sp.sub_stage = 'main'
            __ASSETER_JOIN__
            LEFT JOIN distributor_task_stages m ON m.task_id = t.id AND m.stage_name = 'combiner' AND m.sub_stage = 'main'
            LEFT JOIN distributor_task_stages pub ON pub.task_id = t.id AND pub.stage_name = 'publisher' AND pub.sub_stage = 'main'
            LEFT JOIN publisher_result pr ON pr.task_id = t.id
            LEFT JOIN distributor_task_stages u ON u.task_id = t.id AND u.stage_name = 'uploader' AND u.sub_stage = 'main'
            LEFT JOIN (
              SELECT
                task_id,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) uploader_completed_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) uploader_failed_count,
                SUM(CASE WHEN COALESCE(NULLIF(status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END) uploader_total_count,
                MAX(CASE WHEN platform = 'bilibili' THEN status END) bilibili_upload_status,
                MAX(CASE WHEN platform = 'douyin' THEN status END) douyin_upload_status,
                MAX(CASE WHEN platform = 'xiaohongshu' THEN status END) xiaohongshu_upload_status,
                MAX(CASE WHEN platform = 'shipinhao' THEN status END) shipinhao_upload_status,
                MAX(CASE WHEN platform = 'kuaishou' THEN status END) kuaishou_upload_status,
                MAX(CASE WHEN platform = 'jinritoutiao' THEN status END) jinritoutiao_upload_status,
                MAX(CASE WHEN platform = 'youtube' THEN status END) youtube_upload_status,
                MAX(CASE WHEN platform = 'x' THEN status END) x_upload_status
              FROM uploader_task_status
              GROUP BY task_id
            ) us ON us.task_id = t.id
            LEFT JOIN video_info vi ON vi.task_id = t.id
            LEFT JOIN (
              SELECT task_id,
                     COALESCE(SUM(CASE WHEN stage = 'process_assets' THEN source_bytes ELSE 0 END), 0) minio_storage_bytes,
                     COALESCE(SUM(CASE WHEN stage = 'process_assets' THEN source_object_count ELSE 0 END), 0) minio_storage_object_count,
                     MAX(updated_at) minio_storage_updated_at
              FROM backupper_minio
              GROUP BY task_id
            ) bm ON bm.task_id = t.id
            LEFT JOIN submitter_video sv ON sv.id = vi.submitter_video_id
            __DOWNLOADER_PROGRESS_JOIN__
            LEFT JOIN (
              SELECT task_id, COUNT(*) fixed_count
              FROM whisper_asr_segment
              __ASR_TASK_FILTER__
              GROUP BY task_id
            ) fa ON fa.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) translated_count
              FROM translator_segment
              __TRANSLATOR_SEGMENT_TASK_FILTER__
              GROUP BY task_id
            ) ts ON ts.task_id = t.id
            __TRANSLATOR_CHUNK_JOIN__
            __TRANSLATOR_FAILURE_JOIN__
            LEFT JOIN (
              SELECT
                task_id,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) speaker_completed_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) speaker_failed_count,
                COUNT(*) speaker_total_count
              FROM speaker_segment
              __SPEAKER_SEGMENT_TASK_FILTER__
              GROUP BY task_id
            ) ss ON ss.task_id = t.id
            __TASK_MONITOR_WHERE__
            __TASK_MONITOR_ORDER_BY__
            LIMIT ? OFFSET ?
            """;
    private static final String MONITOR_SUMMARY_SQL = """
            SELECT
              t.id,
              COALESCE(NULLIF(vi.upload_title, ''), NULLIF(sv.title, ''), t.id) title,
              vi.source_url,
              sv.webpage_url source_webpage_url,
              vi.source_thumbnail_url,
              sv.duration source_duration_seconds,
              bm.minio_storage_bytes,
              bm.minio_storage_object_count,
              bm.minio_storage_updated_at,
              vi.type task_type,
              vi.task_type distributor_task_type,
              vi.has_background_audio,
              __DISTRIBUTOR_STAGES_SELECT__
              t.status,
              t.current_stage,
              t.created_at,
              t.started_at,
              t.completed_at,
              t.error_message,

              d.status downloader_status,
              d.started_at downloader_started_at,
              d.completed_at downloader_completed_at,
              d.error_message downloader_error,
              __DOWNLOADER_PROGRESS_SELECT__

              de.status demucs_status,
              de.started_at demucs_started_at,
              de.completed_at demucs_completed_at,
              de.error_message demucs_error,

              w.status whisper_status,
              w.started_at whisper_started_at,
              w.completed_at whisper_completed_at,
              w.error_message whisper_error,

              tr.status translator_status,
              tr.started_at translator_started_at,
              tr.completed_at translator_completed_at,
              tr.error_message translator_error,
              NULL translator_completed_count,
              NULL translator_failed_count,
              NULL translator_total_count,
              NULL translator_child_error,

              sp.status speaker_status,
              sp.started_at speaker_started_at,
              sp.completed_at speaker_completed_at,
              sp.error_message speaker_error,
              NULL speaker_completed_count,
              NULL speaker_failed_count,
              NULL speaker_total_count,
              NULL speaker_child_error,

              __ASSETER_SELECT__

              m.status combiner_status,
              m.started_at combiner_started_at,
              m.completed_at combiner_completed_at,
              m.error_message combiner_error,

              pub.status publisher_status,
              pub.started_at publisher_started_at,
              pub.completed_at publisher_completed_at,
              COALESCE(NULLIF(pub.error_message, ''), pr.error_message) publisher_error,

              CASE WHEN COALESCE(us.uploader_failed_count, 0) > 0 THEN 'failed' ELSE u.status END uploader_status,
              u.started_at uploader_started_at,
              u.completed_at uploader_completed_at,
              COALESCE(us.uploader_completed_count, 0) uploader_completed_count,
              COALESCE(us.uploader_failed_count, 0) uploader_failed_count,
              COALESCE(us.uploader_total_count, 0) uploader_total_count,
              us.bilibili_upload_status,
              us.douyin_upload_status,
              us.xiaohongshu_upload_status,
              us.shipinhao_upload_status,
              us.kuaishou_upload_status,
              us.jinritoutiao_upload_status,
              us.youtube_upload_status,
              us.x_upload_status,
              (
                SELECT submission.account_key
                FROM uploader_task submission
                WHERE submission.task_id = t.id
                  AND submission.platform = 'bilibili'
                ORDER BY FIELD(submission.status, 'success', 'running', 'ready', 'failed'), submission.id DESC
                LIMIT 1
              ) bilibili_upload_account_key,
              (
                SELECT COALESCE(phone_profile.display_name, loginstate.account_key, submission.account_key)
                FROM uploader_task submission
                LEFT JOIN operator_loginstate loginstate
                  ON loginstate.platform = submission.platform
                 AND loginstate.account_key = submission.account_key
                LEFT JOIN (
                  SELECT platform, account_id, MAX(display_name) AS display_name
                  FROM uploader_phone_account
                  GROUP BY platform, account_id
                ) phone_profile
                  ON phone_profile.platform = loginstate.platform
                 AND phone_profile.account_id = loginstate.id
                WHERE submission.task_id = t.id
                  AND submission.platform = 'bilibili'
                ORDER BY FIELD(submission.status, 'success', 'running', 'ready', 'failed'), submission.id DESC
                LIMIT 1
              ) bilibili_upload_account_name,
              u.error_message uploader_error
            FROM task t
            LEFT JOIN distributor_task_stages d ON d.task_id = t.id AND d.stage_name = 'downloader' AND d.sub_stage = 'main'
            LEFT JOIN distributor_task_stages de ON de.task_id = t.id AND de.stage_name = 'demucs' AND de.sub_stage = 'main'
            LEFT JOIN distributor_task_stages w ON w.task_id = t.id AND w.stage_name = 'whisper' AND w.sub_stage = 'main'
            LEFT JOIN distributor_task_stages tr ON tr.task_id = t.id AND tr.stage_name = 'translator' AND tr.sub_stage = 'main'
            LEFT JOIN distributor_task_stages sp ON sp.task_id = t.id AND sp.stage_name = 'speaker' AND sp.sub_stage = 'main'
            __ASSETER_JOIN__
            LEFT JOIN distributor_task_stages m ON m.task_id = t.id AND m.stage_name = 'combiner' AND m.sub_stage = 'main'
            LEFT JOIN distributor_task_stages pub ON pub.task_id = t.id AND pub.stage_name = 'publisher' AND pub.sub_stage = 'main'
            LEFT JOIN publisher_result pr ON pr.task_id = t.id
            LEFT JOIN distributor_task_stages u ON u.task_id = t.id AND u.stage_name = 'uploader' AND u.sub_stage = 'main'
            LEFT JOIN (
              SELECT
                task_id,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) uploader_completed_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) uploader_failed_count,
                SUM(CASE WHEN COALESCE(NULLIF(status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END) uploader_total_count,
                MAX(CASE WHEN platform = 'bilibili' THEN status END) bilibili_upload_status,
                MAX(CASE WHEN platform = 'douyin' THEN status END) douyin_upload_status,
                MAX(CASE WHEN platform = 'xiaohongshu' THEN status END) xiaohongshu_upload_status,
                MAX(CASE WHEN platform = 'shipinhao' THEN status END) shipinhao_upload_status,
                MAX(CASE WHEN platform = 'kuaishou' THEN status END) kuaishou_upload_status,
                MAX(CASE WHEN platform = 'jinritoutiao' THEN status END) jinritoutiao_upload_status,
                MAX(CASE WHEN platform = 'youtube' THEN status END) youtube_upload_status,
                MAX(CASE WHEN platform = 'x' THEN status END) x_upload_status
              FROM uploader_task_status
              GROUP BY task_id
            ) us ON us.task_id = t.id
            LEFT JOIN video_info vi ON vi.task_id = t.id
            LEFT JOIN (
              SELECT task_id,
                     COALESCE(SUM(CASE WHEN stage = 'process_assets' THEN source_bytes ELSE 0 END), 0) minio_storage_bytes,
                     COALESCE(SUM(CASE WHEN stage = 'process_assets' THEN source_object_count ELSE 0 END), 0) minio_storage_object_count,
                     MAX(updated_at) minio_storage_updated_at
              FROM backupper_minio
              GROUP BY task_id
            ) bm ON bm.task_id = t.id
            LEFT JOIN submitter_video sv ON sv.id = vi.submitter_video_id
            __DOWNLOADER_PROGRESS_JOIN__
            __TASK_MONITOR_WHERE__
            __TASK_MONITOR_ORDER_BY__
            LIMIT ? OFFSET ?
            """;
    private static final String MONITOR_COUNT_SQL = """
            SELECT COUNT(*)
            FROM task t
            LEFT JOIN video_info vi ON vi.task_id = t.id
            __TASK_MONITOR_WHERE__
            """;
    private static final String HEARTBEAT_TABLE = "service_heartbeat";
    private static final String HEARTBEAT_TABLE_EXISTS_SQL = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
            """;
    private static final String HEARTBEAT_COLUMNS_SQL = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
            """;


    public MonitorTaskQueryRepositoryServiceImpl(MonitorRepository repository) {
        this(repository, new TaskProgressRouteGraphBuilder(repository));
    }

    @Autowired
    public MonitorTaskQueryRepositoryServiceImpl(MonitorRepository repository, TaskProgressRouteGraphBuilder routeGraphBuilder) {
        super(repository);
        this.routeGraphBuilder = routeGraphBuilder;
    }

    @Override
    public void ensureMonitorSchema() {
        dropColumnIfExists("task", "operator");
        repository.update("""
                CREATE TABLE IF NOT EXISTS distributor_task_stages (
                    task_id VARCHAR(64) NOT NULL,
                    stage_name VARCHAR(64) NOT NULL,
                    sub_stage VARCHAR(64) NOT NULL DEFAULT 'main',
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    started_at DATETIME NULL,
                    completed_at DATETIME NULL,
                    error_message TEXT NULL,
                    `operator` VARCHAR(128) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (task_id, stage_name, sub_stage),
                    KEY idx_distributor_task_stages_stage_status (stage_name, status, sub_stage, task_id),
                    KEY idx_distributor_task_stages_status_operator (status, `operator`, stage_name, updated_at)
                )
                """);
        repository.update("""
                CREATE TABLE IF NOT EXISTS publisher_result (
                    task_id VARCHAR(64) PRIMARY KEY,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    result_json MEDIUMTEXT NULL,
                    error_message TEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """);
        ensurePublisherColumn("result_json", "MEDIUMTEXT NULL");
        ensurePublisherColumn("error_message", "TEXT NULL");
        ensureVideoInfoColumn("upload_title", "VARCHAR(512) NULL");
        ensureVideoInfoColumn("upload_description", "TEXT NULL");
        ensureVideoInfoColumn("upload_tags", "TEXT NULL");
        ensureVideoInfoColumn("cover_text", "VARCHAR(128) NULL");
        ensureVideoInfoColumn("clean_cover_url", "TEXT NULL");
        ensureVideoInfoColumn("final_cover_url", "TEXT NULL");
        ensureVideoInfoColumn("source_cover_url", "TEXT NULL");
        ensureVideoInfoColumn("source_subtitle_txt_url", "TEXT NULL");
        ensureBackupperMinioTable();
    }

    private void ensureBackupperMinioTable() {
        repository.update("""
                CREATE TABLE IF NOT EXISTS backupper_minio (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    task_id VARCHAR(64) NOT NULL,
                    task_type VARCHAR(32) NULL,
                    stage VARCHAR(32) NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
                    started_at DATETIME NULL,
                    finished_at DATETIME NULL,
                    last_error TEXT NULL,
                    location VARCHAR(64) NULL,
                    alidrive_file_id VARCHAR(128) NULL,
                    alidrive_remote_path TEXT NULL,
                    archive_json MEDIUMTEXT NULL,
                    cleaned_at DATETIME NULL,
                    source_object_count INT UNSIGNED NULL,
                    source_bytes BIGINT UNSIGNED NULL,
                    deleted_object_count INT UNSIGNED NULL,
                    deleted_json MEDIUMTEXT NULL,
                    failed_json MEDIUMTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_backupper_minio_task_stage (task_id, stage),
                    KEY idx_backupper_minio_stage_status (stage, status),
                    KEY idx_backupper_minio_task (task_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """);
    }

    private void ensurePublisherColumn(String column, String definition) {
        if (!columnExists("publisher_result", column)) {
            repository.update("ALTER TABLE publisher_result ADD COLUMN " + quotedIdentifier(column) + " " + definition);
            invalidateSchemaCapability("publisher_result", column);
        }
    }

    @Override
    public List<TaskMonitorItem> listTaskMonitorItems(LocalDateTime now, int limit, int offset, String status, String type, String stage, String taskId, String sort) {
        SqlFilter filter = taskMonitorFilter(status, type, stage, taskId);
        List<Object> args = new ArrayList<>(filter.args());
        args.add(limit);
        args.add(offset);
        String sql = monitorSql(MONITOR_SUMMARY_SQL, filter, sort)
                .replace("__TASK_MONITOR_WHERE__", filter.clause())
                .replace("__TASK_MONITOR_ORDER_BY__", monitorOrderBy(sort));
        List<TaskMonitorItem> rows = repository.query(sql, new TaskRowMapper(now), args.toArray());
        return withRouteGraphs(rows, now);
    }

    @Override
    public long countTaskMonitorItems(String status, String type, String stage, String taskId) {
        SqlFilter filter = taskMonitorFilter(status, type, stage, taskId);
        Long count = repository.queryForObject(monitorSql(MONITOR_COUNT_SQL, filter, ""), Long.class, filter.args().toArray());
        return count == null ? 0 : count;
    }

    @Override
    public TaskProgressDetail findTaskProgress(String taskId, LocalDateTime now) {
        SqlFilter filter = new SqlFilter("WHERE t.id = ?", List.of(taskId));
        List<Object> args = new ArrayList<>();
        args.add(taskId); // whisper_asr_segment
        args.add(taskId); // translator_segment
        if (translatorChunkTable() != null) {
            args.add(taskId);
        }
        if (translatorJobTable() != null) {
            args.add(taskId);
        }
        args.add(taskId); // speaker_segment
        args.add(taskId);
        args.add(1);
        args.add(0);
        List<TaskMonitorItem> rows = repository.query(
                progressSql(MONITOR_SQL, filter),
                new TaskRowMapper(now),
                args.toArray()
        );
        if (rows.isEmpty()) {
            return null;
        }
        TaskMonitorItem row = rows.get(0);
        List<StageNode> nodes = withStructuredErrors(taskId, row.nodes());
        TaskProgressRouteGraphBuilder.RouteGraph graph = routeGraphBuilder.build(taskId, nodes, now, true);
        return new TaskProgressDetail(taskId, row.distributorStages(), nodes, graph.nodes(), graph.edges());
    }

    private List<TaskMonitorItem> withRouteGraphs(List<TaskMonitorItem> rows, LocalDateTime now) {
        List<TaskMonitorItem> enriched = new ArrayList<>(rows.size());
        for (TaskMonitorItem row : rows) {
            TaskProgressRouteGraphBuilder.RouteGraph graph = routeGraphBuilder.build(row.taskId(), row.nodes(), now);
            enriched.add(new TaskMonitorItem(
                    row.taskId(), row.title(), row.sourceUrl(), row.sourceWebpageUrl(), row.sourceThumbnailUrl(),
                    row.sourceDurationSeconds(), row.minioStorageBytes(), row.minioStorageObjectCount(),
                    row.minioStorageUpdatedAt(), row.taskType(), row.status(), row.currentStage(), row.createdAt(),
                    row.startedAt(), row.completedAt(), row.elapsedSeconds(), row.bilibiliUploadAccountKey(),
                    row.bilibiliUploadAccountName(), row.errorMessage(), row.distributorStages(), row.nodes(),
                    graph.nodes(), graph.edges()
            ));
        }
        return enriched;
    }

    private List<StageNode> withStructuredErrors(String taskId, List<StageNode> nodes) {
        List<StageNode> enriched = new ArrayList<>(nodes.size());
        for (StageNode node : nodes) {
            if ("translator".equals(node.key())) {
                String translatorJobTable = translatorJobTable();
                enriched.add(withErrors(node, translatorErrors(taskId, translatorJobTable), errorCount(translatorJobTable, taskId)));
            } else if ("speaker".equals(node.key())) {
                enriched.add(withErrors(node, speakerErrors(taskId), errorCount("speaker_segment", taskId)));
            } else {
                enriched.add(node);
            }
        }
        return enriched;
    }

    private List<StageError> translatorErrors(String taskId, String table) {
        if (table == null) {
            return List.of();
        }
        return repository.query("""
                SELECT item_index, COALESCE(NULLIF(error_message, ''), status) message
                FROM %s
                WHERE task_id = ? AND status = 'failed'
                ORDER BY id DESC
                LIMIT 20
                """.formatted(quotedIdentifier(table)),
                (rs, rowNum) -> new StageError(integerOrNull(rs, "item_index"), rs.getString("message")), taskId);
    }

    private List<StageError> speakerErrors(String taskId) {
        return repository.query("""
                SELECT item_index, COALESCE(NULLIF(error_message, ''), status) message
                FROM speaker_segment
                WHERE task_id = ? AND status = 'failed'
                ORDER BY item_index DESC
                LIMIT 20
                """, (rs, rowNum) -> new StageError(integerOrNull(rs, "item_index"), rs.getString("message")), taskId);
    }

    private int errorCount(String table, String taskId) {
        if (table == null || !tableExists(table)) {
            return 0;
        }
        Integer count = repository.queryForObject(
                "SELECT COUNT(*) FROM " + quotedIdentifier(table) + " WHERE task_id = ? AND status = 'failed'",
                Integer.class,
                taskId
        );
        return count == null ? 0 : count;
    }

    private static StageNode withErrors(StageNode node, List<StageError> errors, int errorCount) {
        return new StageNode(
                node.key(), node.label(), node.status(), node.startedAt(), node.completedAt(), node.elapsedSeconds(),
                node.completedCount(), node.failedCount(), node.totalCount(), node.progressPercent(),
                node.errorMessage(), node.childErrorMessage(), node.platformStatuses(), errors, errorCount, errorCount > errors.size()
        );
    }

    private static Integer integerOrNull(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private void ensureVideoInfoColumn(String column, String definition) {
        if (tableExists("video_info") && !columnExists("video_info", column)) {
            repository.update("ALTER TABLE video_info ADD COLUMN " + quotedIdentifier(column) + " " + definition);
            invalidateSchemaCapability("video_info", column);
        }
    }

    private String monitorSql(String template, SqlFilter filter, String sort) {
        return applyCapabilities(template)
                .replace("__ASR_TASK_FILTER__", "")
                .replace("__TRANSLATOR_SEGMENT_TASK_FILTER__", "")
                .replace("__TRANSLATOR_CHUNK_TASK_FILTER__", "")
                .replace("__TRANSLATOR_FAILURE_TASK_FILTER__", "")
                .replace("__SPEAKER_SEGMENT_TASK_FILTER__", "")
                .replace("__TASK_MONITOR_WHERE__", filter.clause())
                .replace("__TASK_MONITOR_ORDER_BY__", monitorOrderBy(sort));
    }

    private String progressSql(String template, SqlFilter filter) {
        return applyCapabilities(template)
                .replace("__TRANSLATOR_CHUNK_JOIN__", translatorChunkJoin())
                .replace("__TRANSLATOR_FAILURE_JOIN__", translatorFailureJoin())
                .replace("__ASR_TASK_FILTER__", "WHERE task_id = ?")
                .replace("__TRANSLATOR_SEGMENT_TASK_FILTER__", "WHERE task_id = ?")
                .replace("__TRANSLATOR_CHUNK_TASK_FILTER__", "ch.task_id = ? AND")
                .replace("__SPEAKER_SEGMENT_TASK_FILTER__", "WHERE task_id = ?")
                .replace("__TASK_MONITOR_WHERE__", filter.clause())
                .replace("__TASK_MONITOR_ORDER_BY__", "");
    }

    private String applyCapabilities(String template) {
        String progressSelect = "NULL downloader_progress_percent,";
        String progressJoin = "";
        if (tableExists("downloader_detail")) {
            progressSelect = "dv.progress_percent downloader_progress_percent,";
            progressJoin = """
                    LEFT JOIN downloader_detail dv
                      ON dv.task_id = t.id
                     AND dv.kind = 'video'
                    """;
        }
        String asseterSelect = """
                NULL asseter_status,
                NULL asseter_started_at,
                NULL asseter_completed_at,
                NULL asseter_error,
                """;
        String asseterJoin = "";
        if (tableExists("distributor_task_stages")) {
            asseterSelect = """
                    a.status asseter_status,
                    a.started_at asseter_started_at,
                    a.completed_at asseter_completed_at,
                    a.error_message asseter_error,
                    """;
            asseterJoin = "LEFT JOIN distributor_task_stages a ON a.task_id = t.id AND a.stage_name = 'asseter' AND a.sub_stage = 'main'";
        }
        String distributorStagesSelect = "NULL distributor_stages,";
        if (tableExists("distributor_type_stages")) {
            distributorStagesSelect = """
                    (
                      SELECT GROUP_CONCAT(dts.stage_name ORDER BY dts.stage_order SEPARATOR ',')
                      FROM distributor_type_stages dts
                      WHERE dts.task_type = vi.task_type
                    ) distributor_stages,
                    """;
        }
        return template
                .replace("__DOWNLOADER_PROGRESS_SELECT__", progressSelect)
                .replace("__DOWNLOADER_PROGRESS_JOIN__", progressJoin)
                .replace("__ASSETER_SELECT__", asseterSelect)
                .replace("__ASSETER_JOIN__", asseterJoin)
                .replace("__DISTRIBUTOR_STAGES_SELECT__", distributorStagesSelect)
                .replace("__TRANSLATOR_CHUNK_JOIN__", translatorChunkJoin());
    }

    private String translatorChunkJoin() {
        String table = translatorChunkTable();
        if (table == null) {
            return """
                    LEFT JOIN (
                      SELECT NULL task_id, NULL translator_completed_count, NULL translator_total_count
                      WHERE FALSE
                    ) tc ON tc.task_id = t.id
                    """;
        }
        return """
                LEFT JOIN (
                  SELECT
                    chunk_progress.task_id,
                    SUM(CASE WHEN chunk_progress.normal_count > 0 AND chunk_progress.translated_count >= chunk_progress.normal_count THEN 1 ELSE 0 END) translator_completed_count,
                    COUNT(*) translator_total_count
                  FROM (
                    SELECT
                      ch.task_id,
                      ch.chunk_index,
                      COUNT(*) normal_count,
                      COUNT(s.task_id) translated_count
                    FROM %s ch
                    LEFT JOIN translator_segment s ON s.task_id = ch.task_id AND s.item_index = ch.item_index
                    WHERE __TRANSLATOR_CHUNK_TASK_FILTER__ ch.row_role = 'normal'
                    GROUP BY ch.task_id, ch.chunk_index
                  ) chunk_progress
                  GROUP BY chunk_progress.task_id
                ) tc ON tc.task_id = t.id
                """.formatted(quotedIdentifier(table));
    }

    private String translatorChunkTable() {
        if (tableExists("translator_chunk")) {
            return "translator_chunk";
        }
        if (tableExists("translator-chunk")) {
            return "translator-chunk";
        }
        return null;
    }

    private String translatorJobTable() {
        if (tableExists("translator_api_task")) {
            return "translator_api_task";
        }
        if (tableExists("translator_jobs")) {
            return "translator_jobs";
        }
        return null;
    }

    private String translatorFailureJoin() {
        String table = translatorJobTable();
        if (table == null) {
            return """
                    LEFT JOIN (
                      SELECT NULL task_id, NULL translator_failed_count
                      WHERE FALSE
                    ) tf ON tf.task_id = t.id
                    """;
        }
        return """
                LEFT JOIN (
                  SELECT task_id, COUNT(DISTINCT item_index) translator_failed_count
                  FROM %s
                  WHERE task_id = ? AND status = 'failed' AND request_key LIKE 'chunk:%%'
                  GROUP BY task_id
                ) tf ON tf.task_id = t.id
                """.formatted(quotedIdentifier(table));
    }

    private static String monitorOrderBy(String sort) {
        String normalized = text(sort);
        if ("minio_storage_desc".equalsIgnoreCase(normalized)) {
            return "ORDER BY COALESCE(bm.minio_storage_bytes, 0) DESC, t.created_at DESC";
        }
        return "ORDER BY t.created_at DESC";
    }

    private static SqlFilter taskMonitorFilter(String status, String type, String stage, String taskId) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        addFilter(conditions, args, "t.status", status);
        addFilter(conditions, args, "vi.type", type);
        addFilter(conditions, args, "t.current_stage", stage);
        String normalizedTaskId = text(taskId);
        if (!normalizedTaskId.isBlank()) {
            conditions.add("t.id LIKE ?");
            args.add("%" + normalizedTaskId + "%");
        }
        if (conditions.isEmpty()) {
            return new SqlFilter("", List.of());
        }
        return new SqlFilter("WHERE " + String.join(" AND ", conditions), args);
    }

    private static void addFilter(List<String> conditions, List<Object> args, String column, String value) {
        String normalized = text(value);
        if (normalized.isBlank() || "all".equalsIgnoreCase(normalized)) {
            return;
        }
        conditions.add(column + " = ?");
        args.add(normalized);
    }

    @Override
    public Map<String, Object> findTaskFlowRow(String table, String idColumn, String id) {
        return singleRow(table, idColumn, id);
    }

    @Override
    public List<Map<String, Object>> listTaskFlowRows(String table, String idColumn, String id, String orderBy, int limit) {
        return rows(table, idColumn, id, orderBy, limit);
    }

    private Map<String, Object> singleRow(String table, String idColumn, String id) {
        if (table == null || !tableExists(table)) {
            return Map.of();
        }
        List<Map<String, Object>> rows = rows(table, idColumn, id, idColumn, 1);
        return rows.isEmpty() ? Map.of() : rows.get(0);
    }

    private List<Map<String, Object>> rows(String table, String idColumn, String id, String orderBy, int limit) {
        return repository.query(
                "SELECT * FROM " + quotedIdentifier(table)
                        + " WHERE " + quotedIdentifier(idColumn) + " = ?"
                        + orderClause(table, orderBy)
                        + " LIMIT ?",
                (rs, rowNum) -> rowMap(rs),
                id,
                limit
        );
    }

    private String orderClause(String table, String orderBy) {
        if (orderBy == null || orderBy.isBlank()) {
            return "";
        }
        Set<String> columns = new HashSet<>(columns(table));
        List<String> parts = new ArrayList<>();
        for (String raw : orderBy.split(",")) {
            String column = raw.trim();
            if (columns.contains(column)) {
                parts.add(quotedIdentifier(column));
            }
        }
        return parts.isEmpty() ? "" : " ORDER BY " + String.join(", ", parts);
    }

    private List<String> columns(String table) {
        if (!tableExists(table)) {
            return List.of();
        }
        return tableColumnsCache.computeIfAbsent(table, key -> repository.queryForList("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """, String.class, key));
    }

    private Map<String, Object> rowMap(ResultSet rs) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        int count = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= count; i++) {
            String name = rs.getMetaData().getColumnLabel(i);
            Object value = rs.getObject(i);
            if (value instanceof Timestamp timestamp) {
                value = timestamp.toLocalDateTime();
            }
            row.put(name, value);
        }
        return row;
    }

    @Override
    public List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now) {
        Map<String, ServiceHeartbeat> byService = new LinkedHashMap<>();
        for (StageDefinition stage : STAGES) {
            byService.put(stage.key(), emptyHeartbeat(stage));
        }

        if (!heartbeatTableExists()) {
            return new ArrayList<>(byService.values());
        }

        repository.query(heartbeatSql(), rs -> {
            String serviceName = rs.getString("service_name");
            if (!byService.containsKey(serviceName)) {
                return;
            }
            String label = labelForService(serviceName);
            byService.put(serviceName, new ServiceHeartbeat(serviceName, label, deviceHeartbeats(rs, now)));
        });
        return new ArrayList<>(byService.values());
    }

    private boolean heartbeatTableExists() {
        Integer count = repository.queryForObject(HEARTBEAT_TABLE_EXISTS_SQL, Integer.class, HEARTBEAT_TABLE);
        return count != null && count > 0;
    }

    private String heartbeatSql() {
        List<String> columns = columns(HEARTBEAT_TABLE);
        StringBuilder sql = new StringBuilder("SELECT service_name");
        for (String device : HEARTBEAT_DEVICES) {
            sql.append(",\n  ");
            if (columns.contains(device)) {
                sql.append(quotedIdentifier(device));
            } else {
                sql.append("NULL AS ").append(quotedIdentifier(device));
            }
        }
        sql.append("\nFROM ").append(HEARTBEAT_TABLE);
        return sql.toString();
    }

    private static ServiceHeartbeat emptyHeartbeat(StageDefinition stage) {
        return new ServiceHeartbeat(stage.key(), stage.label(), emptyDeviceHeartbeats());
    }

    private static List<DeviceHeartbeat> emptyDeviceHeartbeats() {
        List<DeviceHeartbeat> devices = new ArrayList<>();
        for (String device : HEARTBEAT_DEVICES) {
            devices.add(new DeviceHeartbeat(device, null, false, 0));
        }
        return devices;
    }

    private static List<DeviceHeartbeat> deviceHeartbeats(ResultSet rs, LocalDateTime now) throws SQLException {
        List<DeviceHeartbeat> devices = new ArrayList<>();
        for (String device : HEARTBEAT_DEVICES) {
            LocalDateTime lastSeenAt = timestamp(rs, device);
            long secondsSinceLastSeen = elapsedSeconds(lastSeenAt, null, now);
            boolean online = lastSeenAt != null && secondsSinceLastSeen <= HEARTBEAT_ONLINE_SECONDS;
            devices.add(new DeviceHeartbeat(device, lastSeenAt, online, secondsSinceLastSeen));
        }
        return devices;
    }

    private static String labelForService(String serviceName) {
        for (StageDefinition stage : STAGES) {
            if (stage.key().equals(serviceName)) {
                return stage.label();
            }
        }
        return serviceName;
    }

    private static class TaskRowMapper implements RowMapper<TaskMonitorItem> {
        private final LocalDateTime now;

        private TaskRowMapper(LocalDateTime now) {
            this.now = now;
        }

        @Override
        public TaskMonitorItem mapRow(ResultSet rs, int rowNum) throws SQLException {
            LocalDateTime taskStartedAt = timestamp(rs, "started_at");
            LocalDateTime taskCompletedAt = timestamp(rs, "completed_at");
            List<StageNode> nodes = new ArrayList<>();
            for (StageDefinition stage : STAGES) {
                LocalDateTime startedAt = timestamp(rs, stage.startedAtColumn());
                LocalDateTime completedAt = timestamp(rs, stage.completedAtColumn());
                nodes.add(new StageNode(
                        stage.key(),
                        stage.label(),
                        stringOrDefault(rs, stage.statusColumn(), "pending"),
                        startedAt,
                        completedAt,
                        elapsedSeconds(startedAt, completedAt, now),
                        countValue(rs, stage.key(), "completed_count"),
                        countValue(rs, stage.key(), "failed_count"),
                        countValue(rs, stage.key(), "total_count"),
                        progressPercent(rs, stage.key()),
                        rs.getString(stage.errorColumn()),
                        childErrorMessage(rs, stage.key()),
                        platformStatuses(rs, stage.key()),
                        List.of(),
                        0,
                        false
                ));
            }

            return new TaskMonitorItem(
                    rs.getString("id"),
                    rs.getString("title"),
                    rs.getString("source_url"),
                    rs.getString("source_webpage_url"),
                    rs.getString("source_thumbnail_url"),
                    doubleOrNull(rs, "source_duration_seconds"),
                    nullableLong(rs, "minio_storage_bytes"),
                    integerOrNull(rs, "minio_storage_object_count"),
                    timestamp(rs, "minio_storage_updated_at"),
                    rs.getString("task_type"),
                    rs.getString("status"),
                    rs.getString("current_stage"),
                    timestamp(rs, "created_at"),
                    taskStartedAt,
                    taskCompletedAt,
                    elapsedSeconds(taskStartedAt, taskCompletedAt, now),
                    rs.getString("bilibili_upload_account_key"),
                    rs.getString("bilibili_upload_account_name"),
                    rs.getString("error_message"),
                    distributorStages(rs),
                    nodes,
                    List.of(),
                    List.of()
            );
        }

        private static List<String> distributorStages(ResultSet rs) throws SQLException {
            String configuredStages = rs.getString("distributor_stages");
            if (configuredStages == null || configuredStages.isBlank()) {
                return List.of();
            }
            boolean skipDemucs = ("subtitle".equals(rs.getString("distributor_task_type"))
                    || "dubbing".equals(rs.getString("distributor_task_type")))
                    && !rs.getBoolean("has_background_audio")
                    && !rs.wasNull();
            List<String> stages = new ArrayList<>();
            for (String value : configuredStages.split(",")) {
                String stage = value.trim();
                if (!stage.isBlank() && !(skipDemucs && "demucs".equals(stage))) {
                    stages.add(stage);
                }
            }
            return stages;
        }

        private static String stringOrDefault(ResultSet rs, String column, String defaultValue) throws SQLException {
            String value = rs.getString(column);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static Double doubleOrNull(ResultSet rs, String column) throws SQLException {
            double value = rs.getDouble(column);
            return rs.wasNull() ? null : value;
        }

        private static Integer integerOrNull(ResultSet rs, String column) throws SQLException {
            int value = rs.getInt(column);
            return rs.wasNull() ? null : value;
        }

        private static Integer countValue(ResultSet rs, String stageKey, String suffix) throws SQLException {
            if (!"translator".equals(stageKey) && !"speaker".equals(stageKey) && !"uploader".equals(stageKey)) {
                return null;
            }
            int value = rs.getInt(stageKey + "_" + suffix);
            return rs.wasNull() ? null : value;
        }

        private static Double progressPercent(ResultSet rs, String stageKey) throws SQLException {
            if (!"downloader".equals(stageKey)) {
                return null;
            }
            return doubleOrNull(rs, "downloader_progress_percent");
        }

        private static String childErrorMessage(ResultSet rs, String stageKey) throws SQLException {
            if (!"translator".equals(stageKey) && !"speaker".equals(stageKey)) {
                return null;
            }
            return rs.getString(stageKey + "_child_error");
        }

        private static List<UploadPlatformStatus> platformStatuses(ResultSet rs, String stageKey) throws SQLException {
            if (!"uploader".equals(stageKey)) {
                return List.of();
            }
            List<UploadPlatformStatus> statuses = new ArrayList<>();
            addPlatformStatus(statuses, "bilibili", rs.getString("bilibili_upload_status"));
            addPlatformStatus(statuses, "douyin", rs.getString("douyin_upload_status"));
            addPlatformStatus(statuses, "xiaohongshu", rs.getString("xiaohongshu_upload_status"));
            addPlatformStatus(statuses, "shipinhao", rs.getString("shipinhao_upload_status"));
            addPlatformStatus(statuses, "kuaishou", rs.getString("kuaishou_upload_status"));
            addPlatformStatus(statuses, "jinritoutiao", rs.getString("jinritoutiao_upload_status"));
            addPlatformStatus(statuses, "youtube", rs.getString("youtube_upload_status"));
            addPlatformStatus(statuses, "x", rs.getString("x_upload_status"));
            return statuses;
        }

        private static void addPlatformStatus(List<UploadPlatformStatus> statuses, String platform, String rawStatus) {
            String status = rawStatus == null ? "" : rawStatus.trim();
            if (status.isBlank() || "no_need".equals(status)) {
                return;
            }
            statuses.add(new UploadPlatformStatus(platform, status));
        }

    }

    private record SqlFilter(String clause, List<Object> args) {
    }
}
