package com.youbi.monitor.service;

import com.youbi.monitor.model.TaskFlowDetail;
import com.youbi.monitor.repository.IMonitorTaskQueryRepositoryService;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class TaskFlowService {
    private static final List<StageDefinition> STAGES = List.of(
            new StageDefinition("downloader", "下载", "downloader_status", "downloader_started_at", "downloader_completed_at", "downloader_error"),
            new StageDefinition("publisher", "发布准备", "publisher_status", "publisher_started_at", "publisher_completed_at", "publisher_error"),
            new StageDefinition("demucs", "人声分离", "demucs_status", "demucs_started_at", "demucs_completed_at", "demucs_error"),
            new StageDefinition("whisper", "语音识别", "whisper_status", "whisper_started_at", "whisper_completed_at", "whisper_error"),
            new StageDefinition("translator", "翻译", "translator_status", "translator_started_at", "translator_completed_at", "translator_error"),
            new StageDefinition("speaker", "配音", "speaker_status", "speaker_started_at", "speaker_completed_at", "speaker_error"),
            new StageDefinition("asseter", "素材加工", "asseter_status", "asseter_started_at", "asseter_completed_at", "asseter_error"),
            new StageDefinition("combiner", "音视频合成", "combiner_status", "combiner_started_at", "combiner_completed_at", "combiner_error"),
            new StageDefinition("uploader", "上传", "uploader_status", "uploader_started_at", "uploader_completed_at", "uploader_error")
    );
    private static final int CHILD_ROW_LIMIT = 500;
    private static final Map<String, String> STAGE_TABLES = Map.of(
            "downloader", "downloader",
            "demucs", "demucs",
            "whisper", "whisper",
            "translator", "translator",
            "speaker", "speaker",
            "asseter", "asseter",
            "combiner", "combiner",
            "publisher", "publisher",
            "uploader", "uploader"
    );
    private static final Map<String, List<String>> STAGE_INPUT_FIELDS = Map.of(
            "downloader", List.of("source_url"),
            "demucs", List.of("audio_source_url", "audio_source_path"),
            "whisper", List.of("audio_vocals_url", "audio_vocals_path"),
            "translator", List.of("asr_json_path", "target_language"),
            "speaker", List.of("audio_vocals_url", "translation_json_path", "target_language"),
            "combiner", List.of("video_source_url", "audio_bgm_url", "tts_segments_dir", "translation_json_path"),
            "publisher", List.of("title", "source_description", "source_thumbnail_url", "source_subtitle_txt_url"),
            "uploader", List.of("final_video_url", "upload_title", "upload_desc", "upload_tag", "upload_cover_url")
    );
    private static final Map<String, List<String>> STAGE_OUTPUT_FIELDS = Map.of(
            "downloader", List.of("title", "source_duration_seconds", "source_description", "source_uploader", "source_webpage_url", "source_thumbnail_url", "metadata_url", "video_source_url", "audio_source_url"),
            "demucs", List.of("audio_vocals_url", "audio_bgm_url"),
            "whisper", List.of("asr_json_path"),
            "translator", List.of("translation_json_path", "target_language"),
            "speaker", List.of("tts_segments_dir"),
            "combiner", List.of("audio_dubbing_url", "timings_json_path", "final_video_url"),
            "publisher", List.of("upload_title", "upload_description", "upload_tags", "cover_text", "clean_cover_url", "final_cover_url"),
            "uploader", List.of()
    );

    private final IMonitorTaskQueryRepositoryService taskQueryRepositoryService;
    private final TaskAssetService taskAssetService;

    public TaskFlowService(
            IMonitorTaskQueryRepositoryService taskQueryRepositoryService,
            TaskAssetService taskAssetService
    ) {
        this.taskQueryRepositoryService = taskQueryRepositoryService;
        this.taskAssetService = taskAssetService;
    }

    public TaskFlowDetail getTaskFlow(String taskId, String requestedStage) {
        LocalDateTime now = LocalDateTime.now();
        Map<String, Object> task = taskQueryRepositoryService.findTaskFlowRow("task", "id", taskId);
        if (task.isEmpty()) {
            return null;
        }

        Map<String, Object> taskInfo = taskQueryRepositoryService.findTaskFlowRow("task_info", "task_id", taskId);
        enrichSourceMetadata(taskInfo);
        List<String> stageKeys = detailStageKeys(requestedStage);
        List<TaskFlowDetail.TaskFlowAsset> minioObjects = taskAssetService.listTaskAssets(taskId).stream()
                .filter(asset -> stageKeys.contains(asset.stage()))
                .toList();
        List<TaskFlowDetail.TaskFlowStage> stages = new ArrayList<>();
        for (StageDefinition definition : STAGES) {
            if (stageKeys.contains(definition.key())) {
                stages.add(flowStage(taskId, definition, task, taskInfo, minioObjects, now));
            }
        }
        return new TaskFlowDetail(task, taskInfo, stages, minioObjects, now);
    }

    private List<String> detailStageKeys(String requestedStage) {
        String stage = text(requestedStage);
        if (stage.equals("speech") || stage.equals("translator") || stage.equals("speaker")) {
            return List.of("demucs", "whisper", "translator", "speaker");
        }
        boolean knownStage = STAGES.stream().anyMatch(definition -> definition.key().equals(stage));
        return knownStage ? List.of(stage) : List.of("downloader");
    }

    private void enrichSourceMetadata(Map<String, Object> taskInfo) {
        Object submitterVideoId = taskInfo.get("submitter_video_id");
        if (submitterVideoId == null) {
            return;
        }
        Map<String, Object> source = taskQueryRepositoryService.findTaskFlowRow(
                "submitter_video",
                "id",
                String.valueOf(submitterVideoId)
        );
        if (source.isEmpty()) {
            return;
        }
        taskInfo.put("title", source.get("title"));
        taskInfo.put("source_description", source.get("description"));
        taskInfo.put("source_uploader", source.get("uploader"));
        taskInfo.put("source_webpage_url", source.get("webpage_url"));
        taskInfo.put("source_tags_json", source.get("tags"));
        taskInfo.put("source_duration_seconds", source.get("duration"));
    }

    private TaskFlowDetail.TaskFlowStage flowStage(
            String taskId,
            StageDefinition definition,
            Map<String, Object> task,
            Map<String, Object> taskInfo,
            List<TaskFlowDetail.TaskFlowAsset> minioObjects,
            LocalDateTime now
    ) {
        String table = STAGE_TABLES.get(definition.key());
        Map<String, Object> stageRow = mergedStageRow(taskId, definition.key(), table);
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
                flowFields(definition.key(), STAGE_INPUT_FIELDS, task, taskInfo, stageRow, minioObjects),
                flowFields(definition.key(), STAGE_OUTPUT_FIELDS, task, taskInfo, stageRow, minioObjects),
                tables
        );
    }

    private Map<String, Object> mergedStageRow(String taskId, String stageKey, String stageTable) {
        Map<String, Object> physicalRow = taskQueryRepositoryService.findTaskFlowRow(stageTable, "task_id", taskId);
        Map<String, Object> distributorRow = distributorStageRow(taskId, stageKey, "main");
        if (distributorRow.isEmpty()) {
            return physicalRow;
        }
        Map<String, Object> merged = new LinkedHashMap<>(physicalRow);
        for (String column : List.of("status", "started_at", "completed_at", "error_message", "operator", "created_at", "updated_at")) {
            if (distributorRow.containsKey(column)) {
                merged.put(column, distributorRow.get(column));
            }
        }
        if (!merged.containsKey("task_id")) {
            merged.put("task_id", taskId);
        }
        return merged;
    }

    private Map<String, Object> distributorStageRow(String taskId, String stageKey, String subStage) {
        if (!taskQueryRepositoryService.tableExists("distributor_task_stages")) {
            return Map.of();
        }
        return taskQueryRepositoryService.listTaskFlowRows("distributor_task_stages", "task_id", taskId, "stage_name, sub_stage", CHILD_ROW_LIMIT).stream()
                .filter(row -> stageKey.equals(stringValue(row.get("stage_name")))
                        && subStage.equals(stringValue(row.get("sub_stage"))))
                .findFirst()
                .orElse(Map.of());
    }

    private List<TaskFlowDetail.TaskFlowField> flowFields(
            String stageKey,
            Map<String, List<String>> fieldMap,
            Map<String, Object> task,
            Map<String, Object> taskInfo,
            Map<String, Object> stageRow,
            List<TaskFlowDetail.TaskFlowAsset> minioObjects
    ) {
        List<TaskFlowDetail.TaskFlowField> fields = new ArrayList<>();
        for (String name : fieldMap.getOrDefault(stageKey, List.of())) {
            Object value = firstPresent(name, stageRow, taskInfo, task);
            if (isBlankValue(value)) {
                continue;
            }
            fields.add(new TaskFlowDetail.TaskFlowField(name, value, taskAssetService.assetFor(name, stageKey, value, minioObjects)));
        }
        return fields;
    }

    private List<TaskFlowDetail.TaskFlowTable> flowTables(String taskId, String stageKey, String stageTable, Map<String, Object> stageRow) {
        List<TaskFlowDetail.TaskFlowTable> tables = new ArrayList<>();
        if (!stageRow.isEmpty()) {
            tables.add(new TaskFlowDetail.TaskFlowTable(stageTable, List.of(stageRow), false));
        }
        addLimitedTable(tables, "distributor_task_stages", taskId, "task_id", "stage_name, sub_stage");
        switch (stageKey) {
            case "downloader" -> addLimitedTable(tables, "downloader_detail", taskId, "task_id", "kind, id");
            case "whisper" -> {
                addLimitedTable(tables, "whisper_asr_segment", taskId, "task_id", "item_index, id");
                addLimitedTable(tables, "whisper_word_timestamp", taskId, "task_id", "segment_index, word_index, id");
            }
            case "translator" -> {
                addLimitedTable(tables, translatorJobTable(), taskId, "task_id", "id");
                addLimitedTable(tables, translatorChunkTable(), taskId, "task_id", "chunk_index, row_order, id");
                addLimitedTable(tables, "translator_segment", taskId, "task_id", "item_index");
                addLimitedTable(tables, "speaker_segment", taskId, "task_id", "item_index, id");
            }
            case "speaker" -> {
                addLimitedTable(tables, "speaker_segment", taskId, "task_id", "item_index, id");
                addLimitedTable(tables, "product_narration", taskId, "task_id", "id");
                addLimitedTable(tables, "product_narration_sentence", taskId, "task_id", "line_index, id");
            }
            case "combiner" -> {
                addLimitedTable(tables, "combiner_jobs", taskId, "task_id", "sub_stage, id");
                addLimitedTable(tables, "combiner_job", taskId, "task_id", "sub_stage, id");
                addLimitedTable(tables, "product_asmr", taskId, "task_id", "id");
                addLimitedTable(tables, "product_blessing", taskId, "task_id", "id");
            }
            case "publisher" -> {
                addLimitedTable(tables, "publisher_jobs", taskId, "task_id", "job_order, id");
                addLimitedTable(tables, "operator_task", taskId, "task_id", "created_at DESC, id DESC");
                addLimitedTable(tables, "publisher_result", taskId, "task_id", "task_id");
                addLimitedTable(tables, "product_narration", taskId, "task_id", "id");
                addLimitedTable(tables, "product_narration_sentence", taskId, "task_id", "line_index, id");
                addLimitedTable(tables, "product_blessing", taskId, "task_id", "id");
            }
            case "asseter" -> {
                addLimitedTable(tables, "asseter_jobs", taskId, "task_id", "id");
                addLimitedTable(tables, "asseter_static", taskId, "task_id", "id");
            }
            case "uploader" -> {
                addLimitedTable(tables, "uploader", taskId, "task_id", "task_id");
                addLimitedTable(tables, "uploader_task", taskId, "task_id", "platform, topic, id");
            }
            default -> {
            }
        }
        return tables;
    }

    private void addLimitedTable(List<TaskFlowDetail.TaskFlowTable> tables, String table, String id, String idColumn, String orderBy) {
        if (table == null || table.isBlank()) {
            return;
        }
        if (!taskQueryRepositoryService.tableExists(table)) {
            return;
        }
        List<Map<String, Object>> rows = taskQueryRepositoryService.listTaskFlowRows(table, idColumn, id, orderBy, CHILD_ROW_LIMIT + 1);
        boolean truncated = rows.size() > CHILD_ROW_LIMIT;
        if (truncated) {
            rows = rows.subList(0, CHILD_ROW_LIMIT);
        }
        if (!rows.isEmpty()) {
            tables.add(new TaskFlowDetail.TaskFlowTable(table, rows, truncated));
        }
    }

    private String translatorChunkTable() {
        if (taskQueryRepositoryService.tableExists("translator_chunk")) {
            return "translator_chunk";
        }
        if (taskQueryRepositoryService.tableExists("translator-chunk")) {
            return "translator-chunk";
        }
        return null;
    }

    private String translatorJobTable() {
        if (taskQueryRepositoryService.tableExists("translator_api_task")) {
            return "translator_api_task";
        }
        if (taskQueryRepositoryService.tableExists("translator_jobs")) {
            return "translator_jobs";
        }
        return null;
    }

    @SafeVarargs
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
}
