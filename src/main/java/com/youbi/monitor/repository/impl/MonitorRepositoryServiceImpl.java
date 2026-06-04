package com.youbi.monitor.repository.impl;

import com.youbi.monitor.service.MonitorService;

import com.youbi.monitor.dto.DeviceHeartbeat;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.model.TaskMonitorItem;
import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperProcessingDetail.AlignedSegment;
import com.youbi.monitor.model.WhisperProcessingDetail.PysbdSegment;
import com.youbi.monitor.model.WhisperProcessingDetail.RawSegment;
import com.youbi.monitor.model.WhisperProcessingDetail.SplitSegment;
import com.youbi.monitor.model.WhisperWordTimestamp;
import com.youbi.monitor.repository.IMonitorRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class MonitorRepositoryServiceImpl implements IMonitorRepositoryService {
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
            "whisper_word_timestamp",
            "yd_asr_segment"
    );
    private static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_task_bilibili",
            "douyin", "uploader_task_douyin",
            "xiaohongshu", "uploader_task_xiaohongshu",
            "shipinhao", "uploader_task_shipinhao",
            "kuaishou", "uploader_task_kuaishou",
            "jinritoutiao", "uploader_task_jinritoutiao"
    );
    private static final Map<String, String> UPLOADER_ACCOUNT_TABLES = Map.of(
            "bilibili", "uploader_account_bilibili",
            "douyin", "uploader_account_douyin",
            "xiaohongshu", "uploader_account_xiaohongshu",
            "shipinhao", "uploader_account_shipinhao",
            "kuaishou", "uploader_account_kuaishou",
            "jinritoutiao", "uploader_account_jinritoutiao"
    );
    private static final String UNIFIED_UPLOADER_ACCOUNT_TABLE = "uploader_account";
    private static final List<String> PRESERVED_VIDEO_INFO_COLUMNS = List.of(
            "task_id",
            "source_url",
            "source_platform",
            "type",
            "source_uploader",
            "need_subtitle",
            "need_dubbing",
            "need_separation",
            "source_language",
            "target_language",
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
            "translator", List.of("asr_json_path", "target_language"),
            "speaker", List.of("audio_vocals_url", "translation_json_path", "target_language"),
            "combiner", List.of("video_source_url", "audio_bgm_url", "tts_segments_dir", "translation_json_path"),
            "uploader", List.of("final_video_url", "upload_title", "upload_desc", "upload_tag", "upload_cover_url")
    );
    private static final Map<String, List<String>> STAGE_OUTPUT_FIELDS = Map.of(
            "downloader", List.of("title", "source_duration_seconds", "source_description", "source_uploader", "source_webpage_url", "source_thumbnail_url", "metadata_url", "video_source_url", "audio_source_url"),
            "demucs", List.of("audio_vocals_url", "audio_bgm_url"),
            "whisper", List.of("asr_json_path"),
            "translator", List.of("translation_json_path", "target_language"),
            "speaker", List.of("tts_segments_dir"),
            "combiner", List.of("audio_dubbing_url", "timings_json_path", "final_video_url"),
            "uploader", List.of("bilibili_bvid", "bilibili_aid", "upload_result_json", "bilibili_upload_uid", "bilibili_upload_account_name", "shipinhao_upload_account_key", "shipinhao_upload_account_name", "shipinhao_upload_result_json", "kuaishou_upload_account_key", "kuaishou_upload_account_name", "kuaishou_upload_result_json")
    );

    private static final String MONITOR_SQL = """
            SELECT
              t.id,
              COALESCE(NULLIF(u.upload_title, ''), NULLIF(ut.upload_title, ''), NULLIF(t.title, '')) title,
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

              CASE WHEN (
                CASE WHEN COALESCE(NULLIF(u.bilibili_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.douyin_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.xiaohongshu_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.shipinhao_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.kuaishou_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.jinritoutiao_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END
              ) > 0 THEN 'failed' ELSE u.status END uploader_status,
              u.started_at uploader_started_at,
              u.completed_at uploader_completed_at,
              (
                CASE WHEN COALESCE(NULLIF(u.bilibili_upload_status, ''), 'no_need') = 'success' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.douyin_upload_status, ''), 'no_need') = 'success' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.xiaohongshu_upload_status, ''), 'no_need') = 'success' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.shipinhao_upload_status, ''), 'no_need') = 'success' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.kuaishou_upload_status, ''), 'no_need') = 'success' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.jinritoutiao_upload_status, ''), 'no_need') = 'success' THEN 1 ELSE 0 END
              ) uploader_completed_count,
              (
                CASE WHEN COALESCE(NULLIF(u.bilibili_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.douyin_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.xiaohongshu_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.shipinhao_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.kuaishou_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.jinritoutiao_upload_status, ''), 'no_need') = 'failed' THEN 1 ELSE 0 END
              ) uploader_failed_count,
              (
                CASE WHEN COALESCE(NULLIF(u.bilibili_upload_status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.douyin_upload_status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.xiaohongshu_upload_status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.shipinhao_upload_status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.kuaishou_upload_status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(NULLIF(u.jinritoutiao_upload_status, ''), 'no_need') <> 'no_need' THEN 1 ELSE 0 END
              ) uploader_total_count,
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
              SELECT task_id, MIN(title) upload_title
              FROM (
                SELECT task_id, title FROM uploader_task_bilibili WHERE title IS NOT NULL AND title <> ''
                UNION ALL
                SELECT task_id, title FROM uploader_task_douyin WHERE title IS NOT NULL AND title <> ''
                UNION ALL
                SELECT task_id, title FROM uploader_task_xiaohongshu WHERE title IS NOT NULL AND title <> ''
                UNION ALL
                SELECT task_id, title FROM uploader_task_shipinhao WHERE title IS NOT NULL AND title <> ''
                UNION ALL
                SELECT task_id, title FROM uploader_task_kuaishou WHERE title IS NOT NULL AND title <> ''
                UNION ALL
                SELECT task_id, title FROM uploader_task_jinritoutiao WHERE title IS NOT NULL AND title <> ''
              ) upload_titles
              GROUP BY task_id
            ) ut ON ut.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) fixed_count
              FROM yd_asr_segment
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
                UNION ALL
                SELECT task_id, account_key, status, error_message, 'shipinhao' platform FROM uploader_task_shipinhao
                UNION ALL
                SELECT task_id, account_key, status, error_message, 'kuaishou' platform FROM uploader_task_kuaishou
                UNION ALL
                SELECT task_id, account_key, status, error_message, 'jinritoutiao' platform FROM uploader_task_jinritoutiao
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

    private final MonitorRepository repository;

    public MonitorRepositoryServiceImpl(MonitorRepository repository) {
        this.repository = repository;
        ensureMonitorSchema();
    }

    @Override
    public void ensureMonitorSchema() {
    }

    @Override
    public List<TaskMonitorItem> listTaskMonitorItems(LocalDateTime now, int limit) {
        return repository.query(MONITOR_SQL, new TaskRowMapper(now), limit);
    }

    @Override
    public Map<String, Object> findTaskFlowRow(String table, String idColumn, String id) {
        return singleRow(table, idColumn, id);
    }

    @Override
    public List<Map<String, Object>> listTaskFlowRows(String table, String idColumn, String id, String orderBy, int limit) {
        return rows(table, idColumn, id, orderBy, limit);
    }

    @Override
    public List<WhisperWordTimestamp> listWhisperWordTimestamps(String taskId) {
        if (!tableExists("whisper_word_timestamp")) {
            return List.of();
        }
        return repository.query(
                """
                SELECT task_id, segment_index, word_index, text, start_time, end_time
                FROM whisper_word_timestamp
                WHERE task_id = ?
                ORDER BY segment_index, word_index, id
                """,
                (rs, rowNum) -> new WhisperWordTimestamp(
                        rs.getString("task_id"),
                        rs.getInt("segment_index"),
                        rs.getInt("word_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        );
    }

    @Override
    public WhisperProcessingDetail findWhisperProcessing(String taskId) {
        List<WhisperProcessingDetail.RawSegment> rawSegments = tableExists("whisper_raw_segment")
                ? repository.query(
                """
                SELECT id, raw_index, text, start_time, end_time
                FROM whisper_raw_segment
                WHERE task_id = ?
                ORDER BY raw_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.RawSegment(
                        rs.getLong("id"),
                        rs.getInt("raw_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        )
                : List.of();
        List<WhisperProcessingDetail.AlignedSegment> alignedSegments = tableExists("whisper_aligned_segment")
                ? repository.query(
                """
                SELECT id, raw_segment_id, aligned_index, text, start_time, end_time
                FROM whisper_aligned_segment
                WHERE task_id = ?
                ORDER BY aligned_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.AlignedSegment(
                        rs.getLong("id"),
                        nullableLong(rs, "raw_segment_id"),
                        rs.getInt("aligned_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        )
                : List.of();
        List<WhisperProcessingDetail.PysbdSegment> pysbdSegments = tableExists("whisper_pysbd_segment")
                ? repository.query(
                """
                SELECT id, pysbd_index, text, start_time, end_time
                FROM whisper_pysbd_segment
                WHERE task_id = ?
                ORDER BY pysbd_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.PysbdSegment(
                        rs.getLong("id"),
                        rs.getInt("pysbd_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        )
                : List.of();
        List<WhisperProcessingDetail.SplitSegment> splitSegments = tableExists("whisper_split")
                ? repository.query(
                """
                SELECT id, split_index, pysbd_segment_id, text, start_time, end_time,
                       split_reason, split_method, split_punctuation, split_conjunction
                FROM whisper_split
                WHERE task_id = ?
                ORDER BY split_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.SplitSegment(
                        rs.getLong("id"),
                        rs.getInt("split_index"),
                        rs.getLong("pysbd_segment_id"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time"),
                        rs.getString("split_reason"),
                        rs.getString("split_method"),
                        rs.getString("split_punctuation"),
                        rs.getString("split_conjunction")
                ),
                taskId
        )
                : List.of();
        return new WhisperProcessingDetail(rawSegments, alignedSegments, pysbdSegments, splitSegments);
    }

    
    public MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText) {
        String normalizedText = dstText == null ? "" : dstText;
        int updated = repository.update("""
                UPDATE yd_speaker_segment
                SET dst_text = ?
                WHERE task_id = ? AND id = ?
                """, normalizedText, taskId, segmentId);
        if (updated == 0) {
            return null;
        }
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT *
                FROM yd_speaker_segment
                WHERE task_id = ? AND id = ?
                LIMIT 1
                """, taskId, segmentId);
        Map<String, Object> row = rows.isEmpty() ? Map.of() : rows.get(0);
        return new MonitorService.SpeakerSegmentTextUpdateResult(
                segmentId,
                taskId,
                row.get("item_index") instanceof Number itemIndex ? itemIndex.intValue() : null,
                stringValue(row.get("dst_text")),
                localDateTime(row.get("updated_at"))
        );
    }

    
    public MonitorService.FailedUploadSubmissionList listFailedUploadSubmissions(String platform) {
        String normalized = normalizeUploadPlatform(platform);
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        boolean accountTableExists = tableExists(accountTable);
        String accountJoin = accountTableExists
                ? "LEFT JOIN " + quotedIdentifier(accountTable) + " account ON account.account_key = submission.account_key"
                : "";
        String accountExistsSql = accountTableExists ? "account.account_key IS NOT NULL" : "FALSE";
        List<MonitorService.FailedUploadSubmission> rows = repository.query("""
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
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }

        String placeholders = placeholders(normalizedIds.size());
        List<UploadAccountStatusChange> accountStatusChanges = repository.query("""
                SELECT submission.account_key, submission.status
                FROM %s submission
                JOIN %s account ON account.account_key = submission.account_key
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                FOR UPDATE
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders),
                (rs, rowNum) -> new UploadAccountStatusChange(
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
                    submission.error_message = NULL
                WHERE submission.id IN (%s)
                  AND submission.status = 'failed'
                """.formatted(quotedIdentifier(table), quotedIdentifier(accountTable), placeholders), normalizedIds.toArray());
        if (retried > 0) {
            applyUploaderAccountStatusChanges(normalized, accountStatusChanges);
        }

        String taskPlaceholders = placeholders(taskIds.size());
        Object[] taskArgs = taskIds.toArray();
        int uploaderUpdated = repository.update("""
                UPDATE yd_uploader
                SET status = 'running',
                    %s = 'ready',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id IN (%s)
                """.formatted(quotedIdentifier(uploadStatusColumn(normalized)), taskPlaceholders), taskArgs);
        int taskUpdated = repository.update("""
                UPDATE yd_task
                SET status = 'running',
                    current_stage = 'uploader',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
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
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }

        List<MonitorService.UploadBackfillCandidate> rows = repository.query("""
                SELECT
                  task.id task_id,
                  COALESCE(NULLIF(target.title, ''), NULLIF(uploader.upload_title, ''), NULLIF(task.title, ''), task.id) title,
                  COALESCE(NULLIF(target.cover_url, ''), NULLIF(uploader.upload_cover_url, ''), NULLIF(video_info.source_thumbnail_url, '')) cover_url,
                  %s final_video_ref,
                  COALESCE(uploader.completed_at, task.completed_at, task.created_at) completed_at,
                  GROUP_CONCAT(DISTINCT sent.platform ORDER BY sent.platform SEPARATOR ',') uploaded_platforms,
                  target.id target_submission_id,
                  target.status target_status,
                  platform_account.account_key IS NOT NULL account_exists,
                  COALESCE(account.is_enabled, 0) account_enabled,
                  COALESCE(account.is_available, 0) account_available
                FROM yd_task task
                JOIN yd_video_info video_info ON video_info.task_id = task.id
                JOIN yd_uploader uploader ON uploader.task_id = task.id
                JOIN (
                  %s
                ) sent ON sent.task_id = task.id
                LEFT JOIN %s target ON target.task_id = task.id AND target.account_key = ?
                LEFT JOIN %s platform_account ON platform_account.account_key = ?
                LEFT JOIN %s account ON account.platform = ? AND account.account_key = ?
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
                """.formatted(finalVideoRefSql(), successfulUploadUnion(normalized), quotedIdentifier(table), quotedIdentifier(accountTable), UNIFIED_UPLOADER_ACCOUNT_TABLE),
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
                .map(MonitorRepositoryServiceImpl::text)
                .filter(value -> !value.isBlank())
                .distinct()
                .toList();
        if (normalizedTaskIds.isEmpty()) {
            throw new IllegalArgumentException("No task selected.");
        }
        String table = UPLOADER_TASK_TABLES.get(normalized);
        String accountTable = UPLOADER_ACCOUNT_TABLES.get(normalized);
        if (!tableExists(accountTable)) {
            throw new IllegalArgumentException("Account table does not exist for platform: " + normalized);
        }

        String taskPlaceholders = placeholders(normalizedTaskIds.size());
        Object[] queryArgs = new Object[3 + normalizedTaskIds.size()];
        queryArgs[0] = normalizedAccountKey;
        queryArgs[1] = normalized;
        queryArgs[2] = normalizedAccountKey;
        for (int i = 0; i < normalizedTaskIds.size(); i++) {
            queryArgs[i + 3] = normalizedTaskIds.get(i);
        }
        queryArgs[queryArgs.length - 1] = normalizedType;

        List<UploadBackfillInsertRow> rows = repository.query("""
                SELECT
                  task.id task_id,
                  COALESCE(NULLIF(uploader.upload_title, ''), NULLIF(task.title, ''), task.id) title,
                  %s final_video_ref,
                  COALESCE(NULLIF(uploader.upload_cover_url, ''), NULLIF(video_info.source_thumbnail_url, '')) cover_url,
                  uploader.upload_desc,
                  uploader.upload_tag
                FROM yd_task task
                JOIN yd_video_info video_info ON video_info.task_id = task.id
                JOIN yd_uploader uploader ON uploader.task_id = task.id
                JOIN %s platform_account ON platform_account.account_key = ?
                JOIN %s account ON account.platform = ? AND account.account_key = platform_account.account_key AND account.is_enabled = 1 AND account.is_available = 1
                JOIN (
                  %s
                ) sent ON sent.task_id = task.id
                LEFT JOIN %s target ON target.task_id = task.id AND target.account_key = account.account_key
                WHERE task.id IN (%s)
                  AND video_info.type = ?
                  AND COALESCE(NULLIF(%s, ''), '') <> ''
                  AND target.id IS NULL
                GROUP BY
                  task.id,
                  title,
                  final_video_ref,
                  cover_url,
                  uploader.upload_desc,
                  uploader.upload_tag
                """.formatted(
                        finalVideoRefSql(),
                        quotedIdentifier(accountTable),
                        UNIFIED_UPLOADER_ACCOUNT_TABLE,
                        successfulUploadUnion(normalized),
                        quotedIdentifier(table),
                        taskPlaceholders,
                        finalVideoRefSql()
                ),
                (rs, rowNum) -> new UploadBackfillInsertRow(
                        rs.getString("task_id"),
                        rs.getString("title"),
                        rs.getString("final_video_ref"),
                        rs.getString("cover_url"),
                        rs.getString("upload_desc"),
                        rs.getString("upload_tag")
                ),
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
                        task_id, account_key, status, title, video_url, cover_url, description, tags
                    )
                    VALUES (?, ?, 'ready', ?, ?, ?, ?, ?)
                    """.formatted(quotedIdentifier(table)),
                    row.taskId(),
                    normalizedAccountKey,
                    row.title(),
                    row.finalVideoUrl(),
                    row.coverUrl(),
                    row.description(),
                    row.tags()
            );
            registered += inserted;
            if (inserted > 0) {
                accountStatusChanges.add(new UploadAccountStatusChange(normalizedAccountKey, null, "ready"));
            }
        }
        applyUploaderAccountStatusChanges(normalized, accountStatusChanges);

        List<String> registeredTaskIds = rows.stream().map(UploadBackfillInsertRow::taskId).distinct().toList();
        Object[] registeredArgs = registeredTaskIds.toArray();
        String registeredPlaceholders = placeholders(registeredTaskIds.size());
        int uploaderUpdated = repository.update("""
                UPDATE yd_uploader
                SET status = 'running',
                    %s = 'ready',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
                WHERE task_id IN (%s)
                """.formatted(quotedIdentifier(uploadStatusColumn(normalized)), registeredPlaceholders), registeredArgs);
        int taskUpdated = repository.update("""
                UPDATE yd_task
                SET status = 'running',
                    current_stage = 'uploader',
                    started_at = COALESCE(started_at, NOW()),
                    completed_at = NULL,
                    error_message = NULL,
                    `operator` = NULL
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

    
    public MonitorService.SubmitterAuthorType findSubmitterAuthorType(String author) {
        String normalized = text(author);
        if (normalized.isBlank()) {
            return new MonitorService.SubmitterAuthorType("", "", true, true, true, "英文", "中文");
        }
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT type, need_subtitle, need_dubbing, need_separation, source_language, target_language
                FROM submitter_author
                WHERE author = ?
                LIMIT 1
                """, normalized);
        if (rows.isEmpty()) {
            return new MonitorService.SubmitterAuthorType(normalized, "", true, true, true, "英文", "中文");
        }
        Map<String, Object> row = rows.get(0);
        boolean needSubtitle = boolValue(row.get("need_subtitle"), true);
        return new MonitorService.SubmitterAuthorType(
                normalized,
                stringValue(row.get("type")),
                needSubtitle,
                needSubtitle && boolValue(row.get("need_dubbing"), true),
                boolValue(row.get("need_separation"), true),
                defaultLanguage(row.get("source_language"), "英文"),
                defaultLanguage(row.get("target_language"), "中文")
        );
    }

    
    public List<MonitorService.SubmitterAuthorType> listSubmitterAuthorTypes() {
        return repository.query("""
                SELECT author, type, need_subtitle, need_dubbing, need_separation, source_language, target_language
                FROM submitter_author
                ORDER BY CASE WHEN COALESCE(NULLIF(type, ''), '') = '' THEN 1 ELSE 0 END, type, author
                """, (rs, rowNum) -> new MonitorService.SubmitterAuthorType(
                text(rs.getString("author")),
                text(rs.getString("type")),
                boolValue(rs.getObject("need_subtitle"), true),
                boolValue(rs.getObject("need_subtitle"), true) && boolValue(rs.getObject("need_dubbing"), true),
                boolValue(rs.getObject("need_separation"), true),
                defaultLanguage(rs.getString("source_language"), "英文"),
                defaultLanguage(rs.getString("target_language"), "中文")
        ));
    }

    
    public MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            String author,
            String type,
            Boolean needSubtitle,
            Boolean needDubbing,
            Boolean needSeparation,
            String sourceLanguage,
            String targetLanguage
    ) {
        String normalizedAuthor = text(author);
        String normalizedType = text(type);
        String normalizedSourceLanguage = defaultLanguage(sourceLanguage, "英文");
        String normalizedTargetLanguage = defaultLanguage(targetLanguage, "中文");
        if (normalizedAuthor.isBlank()) {
            throw new IllegalArgumentException("author is required");
        }
        if (normalizedType.isBlank()) {
            throw new IllegalArgumentException("type is required");
        }
        boolean normalizedNeedSubtitle = !Boolean.FALSE.equals(needSubtitle);
        boolean normalizedNeedDubbing = normalizedNeedSubtitle && !Boolean.FALSE.equals(needDubbing);
        boolean normalizedNeedSeparation = !Boolean.FALSE.equals(needSeparation);
        repository.update("""
                INSERT INTO submitter_author (author, type, need_subtitle, need_dubbing, need_separation, source_language, target_language)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    type = VALUES(type),
                    need_subtitle = VALUES(need_subtitle),
                    need_dubbing = VALUES(need_dubbing),
                    need_separation = VALUES(need_separation),
                    source_language = VALUES(source_language),
                    target_language = VALUES(target_language),
                    updated_at = NOW()
                """,
                normalizedAuthor,
                normalizedType,
                normalizedNeedSubtitle ? 1 : 0,
                normalizedNeedDubbing ? 1 : 0,
                normalizedNeedSeparation ? 1 : 0,
                normalizedSourceLanguage,
                normalizedTargetLanguage
        );
        return new MonitorService.SubmitterAuthorType(
                normalizedAuthor,
                normalizedType,
                normalizedNeedSubtitle,
                normalizedNeedDubbing,
                normalizedNeedSeparation,
                normalizedSourceLanguage,
                normalizedTargetLanguage
        );
    }

    
    public MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(String author) {
        String normalizedAuthor = text(author);
        if (normalizedAuthor.isBlank()) {
            throw new IllegalArgumentException("author is required");
        }
        int deletedAuthorRows = repository.update("DELETE FROM submitter_author WHERE author = ?", normalizedAuthor);
        int deletedVideoRows = 0;
        if (tableExists("submitter_video")) {
            deletedVideoRows = repository.update("""
                    DELETE FROM submitter_video
                    WHERE uploader = ? OR import_author = ? OR channel_id = ?
                    """, normalizedAuthor, normalizedAuthor, normalizedAuthor);
        }
        return new MonitorService.SubmitterAuthorDeleteResult("deleted", normalizedAuthor, deletedAuthorRows, deletedVideoRows);
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
        return repository.queryForList("""
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

    @Transactional
    
    public boolean markTaskReady(String taskId) {
        RetryStage failedStage = findFailedStage(taskId);
        if (failedStage == null) {
            return false;
        }

        resetFailedStage(taskId, failedStage);
        resetDownstreamStages(taskId, failedStage);
        resetStageChildren(taskId, failedStage);
        repository.update("""
                UPDATE yd_task
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
                "SELECT status FROM yd_task WHERE id = ?",
                String.class,
                taskId
        );
        return statuses.isEmpty() ? null : statuses.get(0);
    }

    @Transactional
    
    public MonitorService.TaskStopResult stopTask(String taskId) {
        List<String> statuses = repository.queryForList(
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

        if (tableExists("yd_translator_api_task")) {
            ensureOperatorColumn("yd_translator_api_task");
            stoppedStages += repository.update("""
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
            stoppedStages += repository.update("""
                    UPDATE yd_speaker_segment
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
                UPDATE yd_task
                SET status = 'failed',
                    current_stage = COALESCE(NULLIF(?, ''), current_stage),
                    completed_at = NOW(),
                    error_message = ?,
                    `operator` = NULL
                WHERE id = ?
                """, stoppedStage, message, taskId);
        return new MonitorService.TaskStopResult("failed", stoppedStages, true);
    }

    private RetryStage findFailedStage(String taskId) {
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
            if (tableExists("yd_translator_api_task")) {
                ensureOperatorColumn("yd_translator_api_task");
                repository.update("""
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
                repository.update("DELETE FROM yd_speaker_segment WHERE task_id = ?", taskId);
            }
            return;
        }

        if ("speaker".equals(failedStage.key()) && tableExists("yd_speaker_segment")) {
            ensureOperatorColumn("yd_speaker_segment");
            repository.update("""
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
                List<UploadAccountStatusChange> accountStatusChanges = repository.query("""
                        SELECT submission.account_key, submission.status
                        FROM %s submission
                        JOIN yd_video_info video_info ON video_info.task_id = submission.task_id
                        LEFT JOIN yd_uploader uploader ON uploader.task_id = submission.task_id
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
                                rs.getString("account_key"),
                                rs.getString("status"),
                                "ready"
                        ),
                        taskId,
                        platform
                );
                repository.update("""
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
                        """.formatted(quotedIdentifier(table)), taskId, platform);
                applyUploaderAccountStatusChanges(platform, accountStatusChanges);
                repository.update("""
                        UPDATE yd_uploader
                        SET %s = 'ready'
                        WHERE task_id = ?
                        """.formatted(quotedIdentifier(uploadStatusColumn(platform))), taskId);
            });
        }
    }

    private void applyUploaderAccountStatusChanges(String platform, List<UploadAccountStatusChange> changes) {
        String normalizedPlatform = normalizeUploadPlatform(platform);
        if (changes == null || changes.isEmpty() || !tableExists(UNIFIED_UPLOADER_ACCOUNT_TABLE)) {
            return;
        }
        boolean hasFailedUploadCount = columnExists(UNIFIED_UPLOADER_ACCOUNT_TABLE, "failed_upload_count");
        Map<UploadAccountStatusDeltaKey, Integer> counts = new LinkedHashMap<>();
        for (UploadAccountStatusChange change : changes) {
            String accountKey = text(change.accountKey());
            if (accountKey.isBlank()) {
                continue;
            }
            UploadAccountStatusDeltaKey key = new UploadAccountStatusDeltaKey(
                    accountKey,
                    normalizeStatus(change.oldStatus()),
                    normalizeStatus(change.newStatus())
            );
            counts.put(key, counts.getOrDefault(key, 0) + 1);
        }
        for (Map.Entry<UploadAccountStatusDeltaKey, Integer> entry : counts.entrySet()) {
            UploadAccountStatusDeltaKey key = entry.getKey();
            int count = entry.getValue();
            ensureUploaderAccountRow(normalizedPlatform, key.accountKey());
            int runningDelta = (runningContribution(key.newStatus()) - runningContribution(key.oldStatus())) * count;
            int failedDelta = (failedContribution(key.newStatus()) - failedContribution(key.oldStatus())) * count;
            int todayDelta = (successContribution(key.newStatus()) - successContribution(key.oldStatus())) * count;
            int waitingDelta = (runningContribution(key.newStatus()) - runningContribution(key.oldStatus())) * count;
            int readyDelta = (readyContribution(key.newStatus()) - readyContribution(key.oldStatus())) * count;
            if (runningDelta == 0 && failedDelta == 0 && todayDelta == 0 && waitingDelta == 0 && readyDelta == 0) {
                continue;
            }

            List<Object> args = new ArrayList<>();
            args.add(runningDelta);
            if (hasFailedUploadCount) {
                args.add(failedDelta);
            }
            args.add(todayDelta);
            args.add(waitingDelta);
            args.add(readyDelta);
            args.add(normalizedPlatform);
            args.add(key.accountKey());
            repository.update("""
                    UPDATE %s
                    SET upload_running_count = GREATEST(0, upload_running_count + ?),
                        %s
                        today_upload_count = GREATEST(0, today_upload_count + ?),
                        cooldown_waiting_count = GREATEST(
                            0,
                            cooldown_waiting_count
                            + ?
                            + CASE WHEN next_upload_allowed_at > NOW() THEN ? ELSE 0 END
                        ),
                        metrics_updated_at = NOW(),
                        updated_at = NOW()
                    WHERE platform = ? AND account_key = ?
                    """.formatted(
                            quotedIdentifier(UNIFIED_UPLOADER_ACCOUNT_TABLE),
                            hasFailedUploadCount
                                    ? "failed_upload_count = GREATEST(0, failed_upload_count + ?),"
                                    : ""
                    ),
                    args.toArray()
            );
        }
    }

    private void ensureUploaderAccountRow(String platform, String accountKey) {
        repository.update("""
                INSERT INTO %s (
                    platform, account_key, source_table, is_enabled, is_available, updated_at
                )
                VALUES (?, ?, ?, 1, 1, NOW())
                ON DUPLICATE KEY UPDATE updated_at = updated_at
                """.formatted(quotedIdentifier(UNIFIED_UPLOADER_ACCOUNT_TABLE)),
                platform,
                accountKey,
                UPLOADER_ACCOUNT_TABLES.get(platform)
        );
    }

    private boolean columnExists(String table, String column) {
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """, Integer.class, table, column);
        return count != null && count > 0;
    }

    private static String normalizeStatus(String status) {
        return text(status).toLowerCase();
    }

    private static int runningContribution(String status) {
        return "running".equals(status) ? 1 : 0;
    }

    private static int failedContribution(String status) {
        return "failed".equals(status) ? 1 : 0;
    }

    private static int successContribution(String status) {
        return "success".equals(status) ? 1 : 0;
    }

    private static int readyContribution(String status) {
        return "ready".equals(status) ? 1 : 0;
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
        if (tableExists("yd_speaker_segment")) {
            Integer count = repository.queryForObject(
                    "SELECT COUNT(*) FROM yd_speaker_segment WHERE task_id = ? AND status = 'running'",
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

    @Override
    public int deleteTaskRows(String taskId) {
        int deleted = 0;
        for (String table : taskScopedTables()) {
            deleted += repository.update("DELETE FROM " + quotedIdentifier(table) + " WHERE task_id = ?", taskId);
        }
        deleted += repository.update("DELETE FROM yd_task WHERE id = ?", taskId);
        return deleted;
    }

    private List<String> taskScopedTables() {
        return repository.queryForList("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND COLUMN_NAME = 'task_id'
                  AND (TABLE_NAME LIKE 'yd\\_%' OR TABLE_NAME = 'downloader_submission')
                ORDER BY CASE
                    WHEN TABLE_NAME IN ('yd_translator_api_task', 'yd_speaker_segment', 'yd_asr_segment') THEN 0
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
            restoreVideoInfoProcessingOptions(taskId);
            return;
        }
        repository.update("UPDATE yd_video_info SET " + nullAssignments(resetColumns) + " WHERE task_id = ?", taskId);
        restoreVideoInfoSource(taskId);
        restoreVideoInfoProcessingOptions(taskId);
    }

    private void restoreVideoInfoSource(String taskId) {
        repository.update("""
                INSERT INTO yd_video_info (task_id, source_url, source_platform)
                SELECT id, source_url, source_platform
                FROM yd_task
                WHERE id = ?
                ON DUPLICATE KEY UPDATE
                    source_url = COALESCE(yd_video_info.source_url, VALUES(source_url)),
                    source_platform = COALESCE(yd_video_info.source_platform, VALUES(source_platform))
                """, taskId);
    }

    private void restoreVideoInfoProcessingOptions(String taskId) {
        if (tableExists("downloader_submission")) {
            repository.update("""
                    UPDATE yd_video_info video_info
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
                    UPDATE yd_video_info video_info
                    JOIN submitter_author author
                      ON author.author = video_info.source_uploader
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
        } else if ("sph".equals(normalized) || "channels".equals(normalized) || "wx_channels".equals(normalized) || "wechat_channels".equals(normalized) || "weixin-channels".equals(normalized) || "视频号".equals(normalized)) {
            normalized = "shipinhao";
        } else if ("ks".equals(normalized) || "kwai".equals(normalized) || "快手".equals(normalized)) {
            normalized = "kuaishou";
        } else if ("toutiao".equals(normalized) || "tt".equals(normalized) || "今日头条".equals(normalized)) {
            normalized = "jinritoutiao";
        }
        if (!UPLOADER_TASK_TABLES.containsKey(normalized)) {
            throw new IllegalArgumentException("Unsupported upload platform: " + platform);
        }
        return normalized;
    }

    private static String successfulUploadUnion(String excludedPlatform) {
        return UPLOADER_TASK_TABLES.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(excludedPlatform))
                .map(entry -> "SELECT task_id, '" + entry.getKey() + "' platform FROM " + quotedIdentifier(entry.getValue()) + " WHERE status = 'success'")
                .reduce((left, right) -> left + "\nUNION ALL\n" + right)
                .orElse("SELECT NULL task_id, NULL platform WHERE FALSE");
    }

    private static String uploadStatusColumn(String platform) {
        return normalizeUploadPlatform(platform) + "_upload_status";
    }

    private static String finalVideoRefSql() {
        return """
                COALESCE(
                  NULLIF(video_info.final_video_url, ''),
                  NULLIF(CONCAT('adrive://', COALESCE(
                    NULLIF(uploader.final_video_alidrive_remote_path, ''),
                    NULLIF(uploader.alidrive_final_video_remote_path, ''),
                    NULLIF(uploader.final_video_alidrive_file_id, ''),
                    NULLIF(uploader.alidrive_final_video_file_id, '')
                  )), 'adrive://'),
                  ''
                )
                """;
    }

    private static List<String> splitCsv(String value) {
        String normalized = text(value);
        if (normalized.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(normalized.split(","))
                .map(MonitorRepositoryServiceImpl::text)
                .filter(item -> !item.isBlank())
                .distinct()
                .toList();
    }

    private static String minioPrefix(String taskId) {
        String clean = text(taskId).replaceFirst("^/+", "").replaceFirst("/+$", "");
        if (clean.isBlank()) {
            throw new IllegalArgumentException("Missing taskId");
        }
        return clean + "/";
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
        List<String> columns = repository.queryForList(HEARTBEAT_COLUMNS_SQL, String.class, HEARTBEAT_TABLE);
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
    }

    private void ensureUploaderSubmissionMonitorSchema() {
    }

    private void ensureVideoInfoMonitorColumns() {
    }

    private void ensureSubmitterAuthorTypeSchema() {
    }

    @Override
    public boolean tableExists(String table) {
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                """, Integer.class, table);
        return count != null && count > 0;
    }

    private void ensureOperatorColumn(String table) {
    }

    private void ensureColumn(String table, String column, String definition) {
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

    private record StageDefinition(
            String key,
            String label,
            String statusColumn,
            String startedAtColumn,
            String completedAtColumn,
            String errorColumn
    ) {
    }

    private record UploadBackfillInsertRow(
            String taskId,
            String title,
            String finalVideoUrl,
            String coverUrl,
            String description,
            String tags
    ) {
    }

    private record UploadAccountStatusChange(String accountKey, String oldStatus, String newStatus) {
    }

    private record UploadAccountStatusDeltaKey(String accountKey, String oldStatus, String newStatus) {
    }

    private static LocalDateTime timestamp(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
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

    private static String defaultLanguage(Object value, String fallback) {
        String normalized = text(value == null ? null : String.valueOf(value));
        return normalized.isBlank() ? fallback : normalized;
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
                    timestamp(rs, "created_at"),
                    taskStartedAt,
                    taskCompletedAt,
                    elapsedSeconds(taskStartedAt, taskCompletedAt, now),
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
