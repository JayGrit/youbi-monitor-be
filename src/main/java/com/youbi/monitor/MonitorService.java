package com.youbi.monitor;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class MonitorService {
    private static final long HEARTBEAT_ONLINE_SECONDS = 60;
    private static final List<String> HEARTBEAT_DEVICES = List.of(
            "Macbook Air M4",
            "Macmini M2",
            "LPXB",
            "MY_HP",
            "LPXB_HP",
            "TXY"
    );
    private static final List<StageDefinition> STAGES = List.of(
            new StageDefinition("downloader", "下载", "downloader_status", "downloader_started_at", "downloader_completed_at", "downloader_error"),
            new StageDefinition("demucs", "人声分离", "demucs_status", "demucs_started_at", "demucs_completed_at", "demucs_error"),
            new StageDefinition("whisper", "语音识别", "whisper_status", "whisper_started_at", "whisper_completed_at", "whisper_error"),
            new StageDefinition("translator", "翻译", "translator_status", "translator_started_at", "translator_completed_at", "translator_error"),
            new StageDefinition("speaker", "配音", "speaker_status", "speaker_started_at", "speaker_completed_at", "speaker_error"),
            new StageDefinition("combiner", "音视频合成", "combiner_status", "combiner_started_at", "combiner_completed_at", "combiner_error"),
            new StageDefinition("uploader", "上传", "uploader_status", "uploader_started_at", "uploader_completed_at", "uploader_error")
    );
    private static final List<RetryStage> RETRY_STAGES = List.of(
            new RetryStage("downloader", "yd_downloader"),
            new RetryStage("demucs", "yd_demucs"),
            new RetryStage("whisper", "yd_whisper"),
            new RetryStage("translator", "yd_translator"),
            new RetryStage("speaker", "yd_speaker"),
            new RetryStage("combiner", "yd_combiner"),
            new RetryStage("uploader", "yd_uploader")
    );
    private static final List<String> RESET_CHILD_TABLES = List.of(
            "yd_speaker_segment",
            "yd_translator_api_task",
            "yd_asr_segment",
            "yd_asr_result"
    );
    private static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_task_bilibili",
            "douyin", "uploader_task_douyin",
            "xiaohongshu", "uploader_task_xiaohongshu"
    );
    private static final Map<String, String> UPLOADER_ACCOUNT_TABLES = Map.of(
            "bilibili", "uploader_account_bilibili",
            "douyin", "uploader_account_douyin",
            "xiaohongshu", "uploader_account_xiaohongshu"
    );
    private static final List<String> PRESERVED_VIDEO_INFO_COLUMNS = List.of(
            "task_id",
            "source_url",
            "source_platform",
            "type",
            "created_at",
            "updated_at"
    );
    private static final List<String> SYSTEM_STAGE_COLUMNS = List.of(
            "task_id",
            "status",
            "started_at",
            "completed_at",
            "error_message",
            "operator",
            "created_at",
            "updated_at"
    );
    private static final int CHILD_ROW_LIMIT = 500;
    private static final Map<String, String> STAGE_TABLES = Map.of(
            "downloader", "yd_downloader",
            "demucs", "yd_demucs",
            "whisper", "yd_whisper",
            "translator", "yd_translator",
            "speaker", "yd_speaker",
            "combiner", "yd_combiner",
            "uploader", "yd_uploader"
    );
    private static final Map<String, List<String>> STAGE_INPUT_FIELDS = Map.of(
            "downloader", List.of("source_url", "source_platform"),
            "demucs", List.of("audio_source_url", "audio_source_path"),
            "whisper", List.of("audio_vocals_url", "audio_vocals_path"),
            "translator", List.of("asr_fixed_json_path", "asr_json_path", "target_language"),
            "speaker", List.of("audio_vocals_url", "translation_json_path", "target_language"),
            "combiner", List.of("video_source_url", "audio_bgm_url", "tts_segments_dir", "translation_json_path"),
            "uploader", List.of("final_video_url", "upload_title", "upload_desc", "upload_tag", "upload_cover_url")
    );
    private static final Map<String, List<String>> STAGE_OUTPUT_FIELDS = Map.of(
            "downloader", List.of("title", "source_duration_seconds", "source_description", "source_uploader", "source_webpage_url", "source_thumbnail_url", "metadata_url", "video_source_url", "audio_source_url"),
            "demucs", List.of("audio_vocals_url", "audio_bgm_url"),
            "whisper", List.of("asr_json_path", "asr_fixed_json_path"),
            "translator", List.of("translation_json_path", "target_language"),
            "speaker", List.of("tts_segments_dir"),
            "combiner", List.of("audio_dubbing_url", "timings_json_path", "final_video_url"),
            "uploader", List.of("bilibili_bvid", "bilibili_aid", "upload_result_json", "bilibili_upload_uid", "bilibili_upload_account_name")
    );

    private static final String MONITOR_SQL = """
            SELECT
              t.id,
              COALESCE(NULLIF(u.upload_title, ''), NULLIF(t.title, '')) title,
              t.source_url,
              vi.source_webpage_url,
              vi.source_thumbnail_url,
              vi.source_duration_seconds,
              vi.type task_type,
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
              ts.translated_count translator_completed_count,
              tf.translator_failed_count translator_failed_count,
              GREATEST(COALESCE(fa.fixed_count, 0), COALESCE(ts.translated_count, 0)) translator_total_count,
              te.child_error_message translator_child_error,

              CASE WHEN COALESCE(ss.speaker_failed_count, 0) > 0 THEN 'failed' ELSE sp.status END speaker_status,
              sp.started_at speaker_started_at,
              sp.completed_at speaker_completed_at,
              sp.error_message speaker_error,
              ss.speaker_completed_count,
              ss.speaker_failed_count,
              ss.speaker_total_count,
              se.child_error_message speaker_child_error,

              m.status combiner_status,
              m.started_at combiner_started_at,
              m.completed_at combiner_completed_at,
              m.error_message combiner_error,

              CASE WHEN COALESCE(us.upload_failed_count, 0) > 0 THEN 'failed' ELSE u.status END uploader_status,
              u.started_at uploader_started_at,
              u.completed_at uploader_completed_at,
              us.upload_completed_count uploader_completed_count,
              us.upload_failed_count uploader_failed_count,
              us.upload_total_count uploader_total_count,
              ue.child_error_message uploader_child_error,
              u.bilibili_upload_uid,
              u.bilibili_upload_account_name,
              u.error_message uploader_error
            FROM yd_task t
            LEFT JOIN yd_downloader d ON d.task_id = t.id
            LEFT JOIN yd_demucs de ON de.task_id = t.id
            LEFT JOIN yd_whisper w ON w.task_id = t.id
            LEFT JOIN yd_translator tr ON tr.task_id = t.id
            LEFT JOIN yd_speaker sp ON sp.task_id = t.id
            LEFT JOIN yd_combiner m ON m.task_id = t.id
            LEFT JOIN yd_uploader u ON u.task_id = t.id
            LEFT JOIN yd_video_info vi ON vi.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) fixed_count
              FROM yd_asr_segment
              WHERE segment_type = 'fixed'
              GROUP BY task_id
            ) fa ON fa.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) translated_count
              FROM yd_speaker_segment
              GROUP BY task_id
            ) ts ON ts.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) translator_failed_count
              FROM yd_translator_api_task
              WHERE status = 'failed'
              GROUP BY task_id
            ) tf ON tf.task_id = t.id
            LEFT JOIN (
              SELECT
                task_id,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) speaker_completed_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) speaker_failed_count,
                COUNT(*) speaker_total_count
              FROM yd_speaker_segment
              GROUP BY task_id
            ) ss ON ss.task_id = t.id
            LEFT JOIN (
              SELECT
                task_id,
                GROUP_CONCAT(
                  CONCAT(request_key, ' #', item_index, ': ', COALESCE(NULLIF(error_message, ''), status))
                  ORDER BY id
                  SEPARATOR 0x0A
                ) child_error_message
              FROM yd_translator_api_task
              WHERE status = 'failed'
              GROUP BY task_id
            ) te ON te.task_id = t.id
            LEFT JOIN (
              SELECT
                task_id,
                GROUP_CONCAT(
                  CONCAT('segment #', item_index, ': ', COALESCE(NULLIF(error_message, ''), status))
                  ORDER BY item_index
                  SEPARATOR 0x0A
                ) child_error_message
              FROM yd_speaker_segment
              WHERE status = 'failed'
              GROUP BY task_id
            ) se ON se.task_id = t.id
            LEFT JOIN (
              SELECT
                task_id,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) upload_completed_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) upload_failed_count,
                COUNT(*) upload_total_count
              FROM (
                SELECT task_id, status FROM uploader_task_bilibili
                UNION ALL
                SELECT task_id, status FROM uploader_task_douyin
                UNION ALL
                SELECT task_id, status FROM uploader_task_xiaohongshu
              ) upload_task
              GROUP BY task_id
            ) us ON us.task_id = t.id
            LEFT JOIN (
              SELECT
                task_id,
                GROUP_CONCAT(
                  CONCAT(platform, '/', account_key, ': ', COALESCE(NULLIF(error_message, ''), status))
                  ORDER BY platform, account_key
                  SEPARATOR 0x0A
                ) child_error_message
              FROM (
                SELECT task_id, account_key, status, error_message, 'bilibili' platform FROM uploader_task_bilibili
                UNION ALL
                SELECT task_id, account_key, status, error_message, 'douyin' platform FROM uploader_task_douyin
                UNION ALL
                SELECT task_id, account_key, status, error_message, 'xiaohongshu' platform FROM uploader_task_xiaohongshu
              ) upload_task
              WHERE status = 'failed'
              GROUP BY task_id
            ) ue ON ue.task_id = t.id
            ORDER BY t.created_at DESC
            LIMIT ?
            """;
    private static final String HEARTBEAT_TABLE = "yd_service_heartbeat";
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

    private final JdbcTemplate jdbcTemplate;
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String minioBucket;

    public MonitorService(
            JdbcTemplate jdbcTemplate,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.minioEndpoint = text(minioEndpoint);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        ensureUploaderSubmissionMonitorSchema();
        ensureUploaderMonitorColumns();
        ensureVideoInfoMonitorColumns();
        ensureSubmitterAuthorTypeSchema();
    }

    public MonitorResponse listTasks(int limit) {
        LocalDateTime now = LocalDateTime.now();
        List<TaskMonitorItem> tasks = jdbcTemplate.query(MONITOR_SQL, new TaskRowMapper(now), limit);
        List<ServiceHeartbeat> serviceHeartbeats = listServiceHeartbeats(now);
        return new MonitorResponse(tasks, serviceHeartbeats, now);
    }

    public TaskFlowDetail getTaskFlow(String taskId) {
        LocalDateTime now = LocalDateTime.now();
        Map<String, Object> task = singleRow("yd_task", "id", taskId);
        if (task.isEmpty()) {
            return null;
        }

        Map<String, Object> videoInfo = singleRow("yd_video_info", "task_id", taskId);
        List<TaskFlowDetail.TaskFlowAsset> minioObjects = listTaskAssets(taskId);
        List<TaskFlowDetail.TaskFlowStage> stages = new ArrayList<>();
        for (StageDefinition definition : STAGES) {
            stages.add(flowStage(taskId, definition, task, videoInfo, minioObjects, now));
        }
        return new TaskFlowDetail(task, videoInfo, stages, minioObjects, now);
    }

    public SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText) {
        String normalizedText = dstText == null ? "" : dstText;
        int updated = jdbcTemplate.update("""
                UPDATE yd_speaker_segment
                SET dst_text = ?
                WHERE task_id = ? AND id = ?
                """, normalizedText, taskId, segmentId);
        if (updated == 0) {
            return null;
        }
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                SELECT *
                FROM yd_speaker_segment
                WHERE task_id = ? AND id = ?
                LIMIT 1
                """, taskId, segmentId);
        Map<String, Object> row = rows.isEmpty() ? Map.of() : rows.get(0);
        return new SpeakerSegmentTextUpdateResult(
                segmentId,
                taskId,
                row.get("item_index") instanceof Number itemIndex ? itemIndex.intValue() : null,
                stringValue(row.get("dst_text")),
                localDateTime(row.get("updated_at"))
        );
    }

    public FailedUploadSubmissionList failedUploadSubmissions(String platform) {
        String normalized = normalizeUploadPlatform(platform);
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        boolean accountTableExists = tableExists(accountTable);
        String accountJoin = accountTableExists
                ? "LEFT JOIN " + quotedIdentifier(accountTable) + " account ON account.account_key = submission.account_key"
                : "";
        String accountExistsSql = accountTableExists ? "account.account_key IS NOT NULL" : "FALSE";
        List<FailedUploadSubmission> rows = jdbcTemplate.query("""
                SELECT
                  submission.id,
                  submission.task_id,
                  COALESCE(NULLIF(submission.title, ''), NULLIF(uploader.upload_title, ''), NULLIF(task.title, ''), submission.task_id) title,
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
                JOIN yd_task task ON task.id = submission.task_id
                JOIN yd_uploader uploader ON uploader.task_id = submission.task_id
                LEFT JOIN yd_video_info video_info ON video_info.task_id = submission.task_id
                %s
                WHERE submission.status = 'failed'
                ORDER BY submission.updated_at DESC, submission.id DESC
                LIMIT 200
                """.formatted(accountExistsSql, quotedIdentifier(table), accountJoin), (rs, rowNum) -> {
            boolean accountExists = rs.getBoolean("account_exists");
            String retryBlockedReason = accountExists ? "" : "账号表中不存在该 account_key，worker 不会拉取";
            return new FailedUploadSubmission(
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
        return new FailedUploadSubmissionList(normalized, rows.size(), rows);
    }

    @Transactional
    public UploadSubmissionRetryResult retryUploadSubmissions(String platform, List<Long> ids) {
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
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }

        String placeholders = placeholders(normalizedIds.size());
        List<String> taskIds = jdbcTemplate.queryForList("""
                SELECT DISTINCT submission.task_id
                FROM %s submission
                JOIN %s account ON account.account_key = submission.account_key
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders), String.class, normalizedIds.toArray());
        if (taskIds.isEmpty()) {
            return new UploadSubmissionRetryResult(normalized, 0, 0, 0);
        }

        int retried = jdbcTemplate.update("""
                UPDATE %s submission
                JOIN %s account ON account.account_key = submission.account_key
                SET submission.status = 'ready',
                    submission.started_at = NULL,
                    submission.completed_at = NULL,
                    submission.error_message = NULL
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders), normalizedIds.toArray());

        String taskPlaceholders = placeholders(taskIds.size());
        Object[] taskArgs = taskIds.toArray();
        int uploaderUpdated = jdbcTemplate.update("""
                UPDATE yd_uploader
                SET status = 'running',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id IN (%s)
                """.formatted(taskPlaceholders), taskArgs);
        int taskUpdated = jdbcTemplate.update("""
                UPDATE yd_task
                SET status = 'running',
                    current_stage = 'uploader',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE id IN (%s)
                """.formatted(taskPlaceholders), taskArgs);
        return new UploadSubmissionRetryResult(normalized, retried, uploaderUpdated, taskUpdated);
    }

    public SubmitterAuthorType authorType(String author) {
        String normalized = text(author);
        if (normalized.isBlank()) {
            return new SubmitterAuthorType("", "", true);
        }
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                SELECT type, need_dubbing
                FROM submitter_author
                WHERE author = ?
                LIMIT 1
                """, normalized);
        if (rows.isEmpty()) {
            return new SubmitterAuthorType(normalized, "", true);
        }
        Map<String, Object> row = rows.get(0);
        return new SubmitterAuthorType(normalized, stringValue(row.get("type")), boolValue(row.get("need_dubbing"), true));
    }

    public List<SubmitterAuthorType> authorTypes() {
        return jdbcTemplate.query("""
                SELECT author, type, need_dubbing
                FROM submitter_author
                ORDER BY author
                """, (rs, rowNum) -> new SubmitterAuthorType(
                text(rs.getString("author")),
                text(rs.getString("type")),
                boolValue(rs.getObject("need_dubbing"), true)
        ));
    }

    public SubmitterAuthorType saveAuthorType(String author, String type, Boolean needDubbing) {
        String normalizedAuthor = text(author);
        String normalizedType = text(type);
        if (normalizedAuthor.isBlank()) {
            throw new IllegalArgumentException("author is required");
        }
        if (normalizedType.isBlank()) {
            throw new IllegalArgumentException("type is required");
        }
        jdbcTemplate.update("""
                INSERT INTO submitter_author (author, type, need_dubbing)
                VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE type = VALUES(type), need_dubbing = VALUES(need_dubbing), updated_at = NOW()
                """, normalizedAuthor, normalizedType, Boolean.FALSE.equals(needDubbing) ? 0 : 1);
        return new SubmitterAuthorType(normalizedAuthor, normalizedType, !Boolean.FALSE.equals(needDubbing));
    }

    private TaskFlowDetail.TaskFlowStage flowStage(
            String taskId,
            StageDefinition definition,
            Map<String, Object> task,
            Map<String, Object> videoInfo,
            List<TaskFlowDetail.TaskFlowAsset> minioObjects,
            LocalDateTime now
    ) {
        String table = STAGE_TABLES.get(definition.key());
        Map<String, Object> stageRow = singleRow(table, "task_id", taskId);
        LocalDateTime startedAt = localDateTime(stageRow.get("started_at"));
        LocalDateTime completedAt = localDateTime(stageRow.get("completed_at"));
        String status = stringValue(stageRow.getOrDefault("status", "pending"));
        List<TaskFlowDetail.TaskFlowTable> tables = flowTables(taskId, definition.key(), table, stageRow);
        return new TaskFlowDetail.TaskFlowStage(
                definition.key(),
                definition.label(),
                status.isBlank() ? "pending" : status,
                startedAt,
                completedAt,
                elapsedSeconds(startedAt, completedAt, now),
                stringValue(stageRow.get("operator")),
                stringValue(stageRow.get("error_message")),
                flowFields(definition.key(), STAGE_INPUT_FIELDS, task, videoInfo, stageRow, minioObjects),
                flowFields(definition.key(), STAGE_OUTPUT_FIELDS, task, videoInfo, stageRow, minioObjects),
                tables
        );
    }

    private List<TaskFlowDetail.TaskFlowField> flowFields(
            String stageKey,
            Map<String, List<String>> fieldMap,
            Map<String, Object> task,
            Map<String, Object> videoInfo,
            Map<String, Object> stageRow,
            List<TaskFlowDetail.TaskFlowAsset> minioObjects
    ) {
        List<TaskFlowDetail.TaskFlowField> fields = new ArrayList<>();
        for (String name : fieldMap.getOrDefault(stageKey, List.of())) {
            Object value = firstPresent(name, stageRow, videoInfo, task);
            if (isBlankValue(value)) {
                continue;
            }
            fields.add(new TaskFlowDetail.TaskFlowField(name, value, assetFor(name, stageKey, value, minioObjects)));
        }
        return fields;
    }

    private List<TaskFlowDetail.TaskFlowTable> flowTables(String taskId, String stageKey, String stageTable, Map<String, Object> stageRow) {
        List<TaskFlowDetail.TaskFlowTable> tables = new ArrayList<>();
        if (!stageRow.isEmpty()) {
            tables.add(new TaskFlowDetail.TaskFlowTable(stageTable, List.of(stageRow), false));
        }
        switch (stageKey) {
            case "whisper" -> {
                addLimitedTable(tables, "yd_asr_result", taskId, "task_id", "task_id");
                addLimitedTable(tables, "yd_asr_segment", taskId, "task_id", "segment_type, item_index, id");
            }
            case "translator" -> {
                addLimitedTable(tables, "yd_translator_api_task", taskId, "task_id", "id");
                addLimitedTable(tables, "yd_speaker_segment", taskId, "task_id", "item_index, id");
            }
            case "speaker" -> addLimitedTable(tables, "yd_speaker_segment", taskId, "task_id", "item_index, id");
            case "uploader" -> {
                addLimitedTable(tables, "yd_uploader", taskId, "task_id", "task_id");
                UPLOADER_TASK_TABLES.forEach((platform, table) -> addUploaderTaskTable(tables, platform, table, taskId));
            }
            default -> {
            }
        }
        return tables;
    }

    private void addUploaderTaskTable(List<TaskFlowDetail.TaskFlowTable> tables, String platform, String table, String taskId) {
        if (!tableExists(table)) {
            return;
        }
        List<Map<String, Object>> rows = rows(table, "task_id", taskId, "account_key, id", CHILD_ROW_LIMIT + 1);
        boolean truncated = rows.size() > CHILD_ROW_LIMIT;
        if (truncated) {
            rows = rows.subList(0, CHILD_ROW_LIMIT);
        }
        if (rows.isEmpty()) {
            return;
        }
        List<Map<String, Object>> decorated = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> decoratedRow = new LinkedHashMap<>(row);
            decoratedRow.put("platform", platform);
            decorated.add(decoratedRow);
        }
        tables.add(new TaskFlowDetail.TaskFlowTable(table, decorated, truncated));
    }

    private void addLimitedTable(List<TaskFlowDetail.TaskFlowTable> tables, String table, String id, String idColumn, String orderBy) {
        if (!tableExists(table)) {
            return;
        }
        List<Map<String, Object>> rows = rows(table, idColumn, id, orderBy, CHILD_ROW_LIMIT + 1);
        boolean truncated = rows.size() > CHILD_ROW_LIMIT;
        if (truncated) {
            rows = rows.subList(0, CHILD_ROW_LIMIT);
        }
        if (!rows.isEmpty()) {
            tables.add(new TaskFlowDetail.TaskFlowTable(table, rows, truncated));
        }
    }

    private Map<String, Object> singleRow(String table, String idColumn, String id) {
        if (table == null || !tableExists(table)) {
            return Map.of();
        }
        List<Map<String, Object>> rows = rows(table, idColumn, id, idColumn, 1);
        return rows.isEmpty() ? Map.of() : rows.get(0);
    }

    private List<Map<String, Object>> rows(String table, String idColumn, String id, String orderBy, int limit) {
        return jdbcTemplate.query(
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
        return jdbcTemplate.queryForList("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """, String.class, table);
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

    private List<TaskFlowDetail.TaskFlowAsset> listTaskAssets(String taskId) {
        List<TaskFlowDetail.TaskFlowAsset> assets = new ArrayList<>();
        String prefix = minioPrefix(taskId);
        Iterable<Result<Item>> objects = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(minioBucket)
                        .prefix(prefix)
                        .recursive(true)
                        .build()
        );
        try {
            for (Result<Item> result : objects) {
                Item item = result.get();
                assets.add(new TaskFlowDetail.TaskFlowAsset(
                        objectDisplayName(item.objectName()),
                        stageFromObject(item.objectName()),
                        kindForName(item.objectName()),
                        publicObjectUrl(item.objectName()),
                        item.objectName(),
                        item.size(),
                        item.lastModified() == null ? null : LocalDateTime.ofInstant(item.lastModified().toInstant(), ZoneId.systemDefault())
                ));
            }
        } catch (Exception exc) {
            assets.add(new TaskFlowDetail.TaskFlowAsset(
                    "MinIO 列表失败",
                    "",
                    "error",
                    "",
                    prefix,
                    null,
                    null
            ));
        }
        return assets;
    }

    private TaskFlowDetail.TaskFlowAsset assetFor(
            String fieldName,
            String stageKey,
            Object value,
            List<TaskFlowDetail.TaskFlowAsset> minioObjects
    ) {
        String text = stringValue(value);
        if (text.isBlank()) {
            return null;
        }
        String objectName = objectNameFromRef(text);
        if (objectName != null) {
            for (TaskFlowDetail.TaskFlowAsset asset : minioObjects) {
                if (objectName.equals(asset.objectName())) {
                    return new TaskFlowDetail.TaskFlowAsset(
                            fieldName,
                            stageKey,
                            kindForField(fieldName, asset.url()),
                            text.startsWith("http://") || text.startsWith("https://") ? text : asset.url(),
                            objectName,
                            asset.size(),
                            asset.lastModified()
                    );
                }
            }
            String url = text.startsWith("http://") || text.startsWith("https://") ? text : publicObjectUrl(objectName);
            return new TaskFlowDetail.TaskFlowAsset(fieldName, stageKey, kindForField(fieldName, url), url, objectName, null, null);
        }
        if (text.startsWith("http://") || text.startsWith("https://")) {
            return new TaskFlowDetail.TaskFlowAsset(fieldName, stageKey, kindForField(fieldName, text), text, null, null, null);
        }
        return null;
    }

    private Object firstPresent(String name, Map<String, Object>... rows) {
        for (Map<String, Object> row : rows) {
            if (row.containsKey(name) && !isBlankValue(row.get(name))) {
                return row.get(name);
            }
        }
        return null;
    }

    private static boolean isBlankValue(Object value) {
        return value == null || (value instanceof String text && text.isBlank());
    }

    private static LocalDateTime localDateTime(Object value) {
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime();
        }
        return null;
    }

    private static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private static boolean boolValue(Object value, boolean fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        String text = String.valueOf(value).trim().toLowerCase();
        if (text.isBlank()) {
            return fallback;
        }
        return !Set.of("0", "false", "no", "off").contains(text);
    }

    private String publicObjectUrl(String objectName) {
        String endpoint = minioEndpoint.replaceFirst("/+$", "");
        return endpoint + "/" + minioBucket + "/" + objectName.replaceFirst("^/+", "");
    }

    private String objectNameFromRef(String ref) {
        String value = text(ref);
        if (value.isBlank() || value.startsWith("db://")) {
            return null;
        }
        if (value.startsWith("s3://")) {
            String withoutScheme = value.substring("s3://".length());
            int slash = withoutScheme.indexOf('/');
            if (slash > 0) {
                String bucket = withoutScheme.substring(0, slash);
                if (bucket.equals(minioBucket)) {
                    return withoutScheme.substring(slash + 1);
                }
            }
            return null;
        }
        if (value.startsWith(minioBucket + "/")) {
            return value.substring(minioBucket.length() + 1);
        }
        if (value.matches("^[^:/]+/.+")) {
            return value.replaceFirst("^/+", "");
        }
        if (value.startsWith("http://") || value.startsWith("https://")) {
            try {
                String path = URI.create(value).getPath();
                String marker = "/minio/" + minioBucket + "/";
                int markerIndex = path.indexOf(marker);
                if (markerIndex >= 0) {
                    return path.substring(markerIndex + marker.length());
                }
                String bucketPrefix = "/" + minioBucket + "/";
                int bucketIndex = path.indexOf(bucketPrefix);
                if (bucketIndex >= 0) {
                    return path.substring(bucketIndex + bucketPrefix.length());
                }
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        return null;
    }

    private static String stageFromObject(String objectName) {
        String[] parts = objectName.split("/");
        return parts.length >= 2 ? parts[1] : "";
    }

    private static String objectDisplayName(String objectName) {
        int slash = objectName.lastIndexOf('/');
        return slash >= 0 ? objectName.substring(slash + 1) : objectName;
    }

    private static String kindForName(String name) {
        String lower = text(name).toLowerCase();
        if (lower.matches(".*\\.(mp4|mov|m4v|webm)$")) {
            return "video";
        }
        if (lower.matches(".*\\.(wav|mp3|m4a|aac|flac|ogg|webm)$")) {
            return "audio";
        }
        if (lower.matches(".*\\.(png|jpg|jpeg|webp|gif)$")) {
            return "image";
        }
        if (lower.matches(".*\\.(json)$") || lower.startsWith("db://")) {
            return "json";
        }
        if (lower.endsWith(".srt") || lower.endsWith(".vtt") || lower.endsWith(".txt")) {
            return "text";
        }
        return "file";
    }

    private static String kindForField(String fieldName, String name) {
        String lowerField = text(fieldName).toLowerCase();
        if (lowerField.contains("audio") || lowerField.contains("wav")) {
            return "audio";
        }
        if (lowerField.contains("video")) {
            return "video";
        }
        if (lowerField.contains("thumbnail") || lowerField.contains("cover")) {
            return "image";
        }
        return kindForName(name);
    }

    @Transactional
    public boolean markTaskReady(String taskId) {
        RetryStage failedStage = findFailedStage(taskId);
        if (failedStage == null) {
            return false;
        }

        resetFailedStage(taskId, failedStage);
        resetDownstreamStages(taskId, failedStage);
        resetStageChildren(taskId, failedStage);
        jdbcTemplate.update("""
                UPDATE yd_task
                SET status = 'ready',
                    current_stage = ?,
                    completed_at = NULL,
                    error_message = NULL
                WHERE id = ?
                """, failedStage.key(), taskId);
        return true;
    }

    @Transactional
    public TaskRestartResult restartTask(String taskId) throws IOException {
        List<String> statuses = jdbcTemplate.queryForList(
                "SELECT status FROM yd_task WHERE id = ?",
                String.class,
                taskId
        );
        if (statuses.isEmpty()) {
            return null;
        }
        if ("running".equals(statuses.get(0)) || hasRunningStage(taskId)) {
            throw new IllegalStateException("Task is running. Stop the worker or wait for it to finish before restarting.");
        }

        int deletedObjects = deleteTaskObjects(taskId);
        resetTaskRowsForDownloader(taskId);
        return new TaskRestartResult("ready", deletedObjects);
    }

    @Transactional
    public TaskDeleteResult deleteTask(String taskId) throws IOException {
        List<String> statuses = jdbcTemplate.queryForList(
                "SELECT status FROM yd_task WHERE id = ?",
                String.class,
                taskId
        );
        if (statuses.isEmpty()) {
            return null;
        }
        if ("running".equals(statuses.get(0))) {
            throw new IllegalStateException("Task is running. Stop the worker or wait for it to finish before deleting.");
        }

        int deletedObjects = deleteTaskObjects(taskId);
        int deletedRows = deleteTaskRows(taskId);
        return new TaskDeleteResult("deleted", deletedRows, deletedObjects);
    }

    @Transactional
    public TaskStopResult stopTask(String taskId) {
        List<String> statuses = jdbcTemplate.queryForList(
                "SELECT status FROM yd_task WHERE id = ?",
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
            int updated = jdbcTemplate.update("""
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

        if (tableExists("yd_translator_api_task")) {
            ensureOperatorColumn("yd_translator_api_task");
            stoppedStages += jdbcTemplate.update("""
                    UPDATE yd_translator_api_task
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?,
                        `operator` = NULL
                    WHERE task_id = ? AND status IN ('pending', 'running')
                    """, message, taskId);
        }
        if (tableExists("yd_speaker_segment")) {
            ensureOperatorColumn("yd_speaker_segment");
            stoppedStages += jdbcTemplate.update("""
                    UPDATE yd_speaker_segment
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = ?,
                        `operator` = NULL
                    WHERE task_id = ? AND status IN ('ready', 'running')
                    """, message, taskId);
        }

        if (stoppedStages == 0 && !"running".equals(statuses.get(0))) {
            return new TaskStopResult(statuses.get(0), 0, false);
        }

        jdbcTemplate.update("""
                UPDATE yd_task
                SET status = 'failed',
                    current_stage = COALESCE(NULLIF(?, ''), current_stage),
                    completed_at = NOW(),
                    error_message = ?,
                    `operator` = NULL
                WHERE id = ?
                """, stoppedStage, message, taskId);
        return new TaskStopResult("failed", stoppedStages, true);
    }

    private RetryStage findFailedStage(String taskId) {
        for (RetryStage stage : RETRY_STAGES) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM " + stage.table() + " WHERE task_id = ? AND status = 'failed'",
                    Integer.class,
                    taskId
            );
            if (count != null && count > 0) {
                return stage;
            }
        }

        List<String> currentStages = jdbcTemplate.queryForList("""
                SELECT current_stage
                FROM yd_task
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

    private void resetFailedStage(String taskId, RetryStage stage) {
        ensureOperatorColumn(stage.table());
        jdbcTemplate.update("""
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
            jdbcTemplate.update("""
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
            if (tableExists("yd_translator_api_task")) {
                ensureOperatorColumn("yd_translator_api_task");
                jdbcTemplate.update("""
                        UPDATE yd_translator_api_task
                        SET status = 'pending',
                            attempt_count = 0,
                            started_at = NULL,
                            completed_at = NULL,
                            error_message = NULL,
                            next_run_at = NOW(),
                            `operator` = NULL
                        WHERE task_id = ? AND status IN ('failed', 'running')
                        """, taskId);
            }
            if (tableExists("yd_speaker_segment")) {
                jdbcTemplate.update("DELETE FROM yd_speaker_segment WHERE task_id = ?", taskId);
            }
            return;
        }

        if ("speaker".equals(failedStage.key()) && tableExists("yd_speaker_segment")) {
            ensureOperatorColumn("yd_speaker_segment");
            jdbcTemplate.update("""
                    UPDATE yd_speaker_segment
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
                jdbcTemplate.update("""
                        UPDATE %s submission
                        JOIN yd_video_info video_info ON video_info.task_id = submission.task_id
                        LEFT JOIN yd_uploader uploader ON uploader.task_id = submission.task_id
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
                        """.formatted(table), taskId, platform);
            });
        }
    }

    private boolean hasRunningStage(String taskId) {
        for (RetryStage stage : RETRY_STAGES) {
            if (!tableExists(stage.table())) {
                continue;
            }
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM " + quotedIdentifier(stage.table()) + " WHERE task_id = ? AND status = 'running'",
                    Integer.class,
                    taskId
            );
            if (count != null && count > 0) {
                return true;
            }
        }
        if (tableExists("yd_speaker_segment")) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM yd_speaker_segment WHERE task_id = ? AND status = 'running'",
                    Integer.class,
                    taskId
            );
            return count != null && count > 0;
        }
        return false;
    }

    private int deleteTaskObjects(String taskId) throws IOException {
        String prefix = minioPrefix(taskId);
        int deleted = 0;
        Iterable<Result<Item>> objects = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(minioBucket)
                        .prefix(prefix)
                        .recursive(true)
                        .build()
        );
        try {
            for (Result<Item> result : objects) {
                Item item = result.get();
                minioClient.removeObject(
                        RemoveObjectArgs.builder()
                                .bucket(minioBucket)
                                .object(item.objectName())
                                .build()
                );
                deleted++;
            }
        } catch (Exception exc) {
            throw new IOException("Cannot delete MinIO objects under prefix " + prefix, exc);
        }
        return deleted;
    }

    private void resetTaskRowsForDownloader(String taskId) {
        for (String table : RESET_CHILD_TABLES) {
            if (tableExists(table)) {
                jdbcTemplate.update("DELETE FROM " + quotedIdentifier(table) + " WHERE task_id = ?", taskId);
            }
        }

        resetVideoInfo(taskId);
        for (RetryStage stage : RETRY_STAGES) {
            resetStageRow(taskId, stage, "downloader".equals(stage.key()) ? "ready" : "pending");
        }

        jdbcTemplate.update("""
                UPDATE yd_task
                SET status = 'ready',
                    current_stage = 'downloader',
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE id = ?
                """, taskId);
    }

    private int deleteTaskRows(String taskId) {
        int deleted = 0;
        for (String table : taskScopedTables()) {
            deleted += jdbcTemplate.update("DELETE FROM " + quotedIdentifier(table) + " WHERE task_id = ?", taskId);
        }
        deleted += jdbcTemplate.update("DELETE FROM yd_task WHERE id = ?", taskId);
        return deleted;
    }

    private List<String> taskScopedTables() {
        return jdbcTemplate.queryForList("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND COLUMN_NAME = 'task_id'
                  AND (TABLE_NAME LIKE 'yd\\_%' OR TABLE_NAME = 'downloader_submission')
                ORDER BY CASE
                    WHEN TABLE_NAME IN ('yd_translator_api_task', 'yd_speaker_segment', 'yd_asr_segment', 'yd_asr_result') THEN 0
                    WHEN TABLE_NAME = 'downloader_submission' THEN 2
                    ELSE 1
                  END,
                  TABLE_NAME
                """, String.class);
    }

    private void resetVideoInfo(String taskId) {
        if (!tableExists("yd_video_info")) {
            return;
        }
        List<String> resetColumns = resettableColumns("yd_video_info", PRESERVED_VIDEO_INFO_COLUMNS);
        if (resetColumns.isEmpty()) {
            restoreVideoInfoSource(taskId);
            return;
        }
        jdbcTemplate.update("UPDATE yd_video_info SET " + nullAssignments(resetColumns) + " WHERE task_id = ?", taskId);
        restoreVideoInfoSource(taskId);
    }

    private void restoreVideoInfoSource(String taskId) {
        jdbcTemplate.update("""
                INSERT INTO yd_video_info (task_id, source_url, source_platform)
                SELECT id, source_url, source_platform
                FROM yd_task
                WHERE id = ?
                ON DUPLICATE KEY UPDATE
                    source_url = COALESCE(yd_video_info.source_url, VALUES(source_url)),
                    source_platform = COALESCE(yd_video_info.source_platform, VALUES(source_platform))
                """, taskId);
    }

    private void resetStageRow(String taskId, RetryStage stage, String status) {
        if (!tableExists(stage.table())) {
            return;
        }
        ensureOperatorColumn(stage.table());
        List<String> resetColumns = resettableColumns(stage.table(), SYSTEM_STAGE_COLUMNS);
        String extraAssignments = resetColumns.isEmpty() ? "" : ",\n                    " + nullAssignments(resetColumns);
        jdbcTemplate.update("""
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
        return jdbcTemplate.queryForList("""
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

    private static String placeholders(int count) {
        return "?,".repeat(Math.max(0, count)).replaceFirst(",$", "");
    }

    private static String normalizeUploadPlatform(String platform) {
        String normalized = text(platform).toLowerCase();
        if ("bili".equals(normalized)) {
            normalized = "bilibili";
        } else if ("xhs".equals(normalized) || "red".equals(normalized)) {
            normalized = "xiaohongshu";
        } else if ("dy".equals(normalized)) {
            normalized = "douyin";
        }
        if (!UPLOADER_TASK_TABLES.containsKey(normalized)) {
            throw new IllegalArgumentException("Unsupported upload platform: " + platform);
        }
        return normalized;
    }

    private static String minioPrefix(String taskId) {
        String clean = text(taskId).replaceFirst("^/+", "").replaceFirst("/+$", "");
        if (clean.isBlank()) {
            throw new IllegalArgumentException("Missing taskId");
        }
        return clean + "/";
    }

    private List<ServiceHeartbeat> listServiceHeartbeats(LocalDateTime now) {
        Map<String, ServiceHeartbeat> byService = new LinkedHashMap<>();
        for (StageDefinition stage : STAGES) {
            byService.put(stage.key(), emptyHeartbeat(stage));
        }

        if (!heartbeatTableExists()) {
            return new ArrayList<>(byService.values());
        }

        jdbcTemplate.query(heartbeatSql(), rs -> {
            String serviceName = rs.getString("service_name");
            String label = labelForService(serviceName);
            byService.put(serviceName, new ServiceHeartbeat(serviceName, label, deviceHeartbeats(rs, now)));
        });
        return new ArrayList<>(byService.values());
    }

    private boolean heartbeatTableExists() {
        Integer count = jdbcTemplate.queryForObject(HEARTBEAT_TABLE_EXISTS_SQL, Integer.class, HEARTBEAT_TABLE);
        return count != null && count > 0;
    }

    private String heartbeatSql() {
        List<String> columns = jdbcTemplate.queryForList(HEARTBEAT_COLUMNS_SQL, String.class, HEARTBEAT_TABLE);
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

    private static String quotedIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private void ensureUploaderMonitorColumns() {
        ensureColumn("yd_uploader", "bilibili_upload_uid", "VARCHAR(64) NULL");
        ensureColumn("yd_uploader", "bilibili_upload_account_name", "VARCHAR(128) NULL");
        ensureColumn("yd_uploader", "upload_cover_url", "TEXT NULL");
    }

    private void ensureUploaderSubmissionMonitorSchema() {
        UPLOADER_TASK_TABLES.forEach((platform, table) -> {
            jdbcTemplate.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        task_id VARCHAR(64) NOT NULL,
                        account_key VARCHAR(128) NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'ready',
                        request_json MEDIUMTEXT NULL,
                        title VARCHAR(512) NULL,
                        video_url TEXT NULL,
                        cover_url TEXT NULL,
                        description TEXT NULL,
                        tags VARCHAR(512) NULL,
                        result_json MEDIUMTEXT NULL,
                        error_message TEXT NULL,
                        started_at DATETIME NULL,
                        completed_at DATETIME NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_uploader_task (task_id, account_key),
                        KEY idx_uploader_task_status (status, account_key),
                        KEY idx_uploader_task_task (task_id, status)
                    )
                    """.formatted(table));
            ensureColumn(table, "request_json", "MEDIUMTEXT NULL");
            ensureColumn(table, "title", "VARCHAR(512) NULL");
            ensureColumn(table, "video_url", "TEXT NULL");
            ensureColumn(table, "cover_url", "TEXT NULL");
            ensureColumn(table, "description", "TEXT NULL");
            ensureColumn(table, "tags", "VARCHAR(512) NULL");
        });
    }

    private void ensureVideoInfoMonitorColumns() {
        if (!tableExists("yd_video_info")) {
            return;
        }
        ensureColumn("yd_video_info", "source_duration_seconds", "DOUBLE NULL");
        ensureColumn("yd_video_info", "type", "VARCHAR(128) NULL");
        ensureColumn("yd_video_info", "need_dubbing", "TINYINT(1) NULL");
    }

    private void ensureSubmitterAuthorTypeSchema() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS submitter_author (
                    author VARCHAR(255) NOT NULL PRIMARY KEY,
                    type VARCHAR(128) NOT NULL,
                    need_dubbing TINYINT(1) NOT NULL DEFAULT 1,
                    note VARCHAR(255) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    KEY idx_submitter_author_type_type (type)
                )
                """);
        ensureColumn("submitter_author", "need_dubbing", "TINYINT(1) NOT NULL DEFAULT 1");
    }

    private boolean tableExists(String table) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                """, Integer.class, table);
        return count != null && count > 0;
    }

    private void ensureOperatorColumn(String table) {
        ensureColumn(table, "operator", "VARCHAR(128) NULL");
    }

    private void ensureColumn(String table, String column, String definition) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """, Integer.class, table, column);
        if (count != null && count > 0) {
            return;
        }
        jdbcTemplate.execute("ALTER TABLE " + quotedIdentifier(table) + " ADD COLUMN " + quotedIdentifier(column) + " " + definition);
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

    private record RetryStage(String key, String table) {
    }

    public record TaskRestartResult(String status, int deletedMinioObjects) {
    }

    public record TaskStopResult(String status, int stoppedStages, boolean stoppedTask) {
    }

    public record TaskDeleteResult(String status, int deletedDatabaseRows, int deletedMinioObjects) {
    }

    public record SpeakerSegmentTextUpdateResult(
            long id,
            String taskId,
            Integer itemIndex,
            String dstText,
            LocalDateTime updatedAt
    ) {
    }

    public record FailedUploadSubmission(
            long id,
            String platform,
            String taskId,
            String title,
            String accountKey,
            String errorMessage,
            LocalDateTime completedAt,
            LocalDateTime updatedAt,
            String taskStatus,
            String uploaderStatus,
            String uploadPlatforms,
            String routedAccountKey,
            boolean accountExists,
            String retryBlockedReason
    ) {
    }

    public record FailedUploadSubmissionList(String platform, int count, List<FailedUploadSubmission> rows) {
    }

    public record UploadSubmissionRetryRequest(List<Long> ids) {
    }

    public record UploadSubmissionRetryResult(String platform, int retriedCount, int uploaderTaskCount, int taskCount) {
    }

    public record SubmitterAuthorType(String author, String type, boolean needDubbing) {
    }

    public record SubmitterAuthorTypeUpdateRequest(String author, String type, Boolean needDubbing) {
    }

    private static LocalDateTime timestamp(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static long elapsedSeconds(LocalDateTime startedAt, LocalDateTime completedAt, LocalDateTime now) {
        if (startedAt == null) {
            return 0;
        }
        LocalDateTime end = completedAt == null ? now : completedAt;
        return Math.max(0, Duration.between(startedAt, end).getSeconds());
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
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
                        rs.getString(stage.errorColumn()),
                        childErrorMessage(rs, stage.key())
                ));
            }

            return new TaskMonitorItem(
                    rs.getString("id"),
                    rs.getString("title"),
                    rs.getString("source_url"),
                    rs.getString("source_webpage_url"),
                    rs.getString("source_thumbnail_url"),
                    doubleOrNull(rs, "source_duration_seconds"),
                    rs.getString("task_type"),
                    rs.getString("status"),
                    rs.getString("current_stage"),
                    MonitorService.timestamp(rs, "created_at"),
                    taskStartedAt,
                    taskCompletedAt,
                    MonitorService.elapsedSeconds(taskStartedAt, taskCompletedAt, now),
                    rs.getString("bilibili_upload_uid"),
                    rs.getString("bilibili_upload_account_name"),
                    rs.getString("error_message"),
                    nodes
            );
        }

        private static String stringOrDefault(ResultSet rs, String column, String defaultValue) throws SQLException {
            String value = rs.getString(column);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static Double doubleOrNull(ResultSet rs, String column) throws SQLException {
            double value = rs.getDouble(column);
            return rs.wasNull() ? null : value;
        }

        private static Integer countValue(ResultSet rs, String stageKey, String suffix) throws SQLException {
            if (!"translator".equals(stageKey) && !"speaker".equals(stageKey) && !"uploader".equals(stageKey)) {
                return null;
            }
            int value = rs.getInt(stageKey + "_" + suffix);
            return rs.wasNull() ? null : value;
        }

        private static String childErrorMessage(ResultSet rs, String stageKey) throws SQLException {
            if (!"translator".equals(stageKey) && !"speaker".equals(stageKey) && !"uploader".equals(stageKey)) {
                return null;
            }
            return rs.getString(stageKey + "_child_error");
        }

    }
}
