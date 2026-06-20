package com.youbi.monitor.service;

import com.youbi.monitor.dto.MonitorResponse;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.TaskFlowDetail;
import com.youbi.monitor.model.TaskMonitorSummary;
import com.youbi.monitor.model.TaskProgressDetail;
import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperWordTimestamp;
import com.youbi.monitor.repository.IMonitorTaskQueryRepositoryService;
import com.youbi.monitor.repository.ISpeakerSegmentRepositoryService;
import com.youbi.monitor.repository.ISubmitterAuthorRepositoryService;
import com.youbi.monitor.repository.ITaskLifecycleRepositoryService;
import com.youbi.monitor.repository.IUploadSubmissionRepositoryService;
import com.youbi.monitor.repository.IWhisperProcessingRepositoryService;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class MonitorService {
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
    private static final Map<String, String> UPLOADER_TASK_TABLES = Map.of(
            "bilibili", "uploader_task_bilibili",
            "douyin", "uploader_task_douyin",
            "xiaohongshu", "uploader_task_xiaohongshu",
            "shipinhao", "uploader_task_shipinhao",
            "kuaishou", "uploader_task_kuaishou",
            "jinritoutiao", "uploader_task_jinritoutiao"
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
            "uploader", List.of("bilibili_bvid", "bilibili_aid", "upload_result_json", "bilibili_upload_uid", "bilibili_upload_account_name", "shipinhao_upload_account_key", "shipinhao_upload_account_name", "shipinhao_upload_result_json", "kuaishou_upload_account_key", "kuaishou_upload_account_name", "kuaishou_upload_result_json")
    );

    private final IMonitorTaskQueryRepositoryService taskQueryRepositoryService;
    private final IWhisperProcessingRepositoryService whisperProcessingRepositoryService;
    private final ISpeakerSegmentRepositoryService speakerSegmentRepositoryService;
    private final IUploadSubmissionRepositoryService uploadSubmissionRepositoryService;
    private final ISubmitterAuthorRepositoryService submitterAuthorRepositoryService;
    private final ITaskLifecycleRepositoryService taskLifecycleRepositoryService;
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String minioBucket;

    public MonitorService(
            IMonitorTaskQueryRepositoryService taskQueryRepositoryService,
            IWhisperProcessingRepositoryService whisperProcessingRepositoryService,
            ISpeakerSegmentRepositoryService speakerSegmentRepositoryService,
            IUploadSubmissionRepositoryService uploadSubmissionRepositoryService,
            ISubmitterAuthorRepositoryService submitterAuthorRepositoryService,
            ITaskLifecycleRepositoryService taskLifecycleRepositoryService,
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.taskQueryRepositoryService = taskQueryRepositoryService;
        this.whisperProcessingRepositoryService = whisperProcessingRepositoryService;
        this.speakerSegmentRepositoryService = speakerSegmentRepositoryService;
        this.uploadSubmissionRepositoryService = uploadSubmissionRepositoryService;
        this.submitterAuthorRepositoryService = submitterAuthorRepositoryService;
        this.taskLifecycleRepositoryService = taskLifecycleRepositoryService;
        this.minioEndpoint = text(minioEndpoint);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.taskQueryRepositoryService.ensureMonitorSchema();
    }

    public MonitorResponse listTasks(int page, int limit, String status, String type, String stage, String taskId, String sort) {
        LocalDateTime now = LocalDateTime.now();
        int offset = Math.max(0, page - 1) * limit;
        List<TaskMonitorSummary> tasks = taskQueryRepositoryService.listTaskMonitorItems(now, limit, offset, status, type, stage, taskId, sort);
        long totalCount = taskQueryRepositoryService.countTaskMonitorItems(status, type, stage, taskId);
        return new MonitorResponse(tasks, now, page, limit, totalCount);
    }

    public List<ServiceHeartbeat> listServiceHeartbeats() {
        return taskQueryRepositoryService.listServiceHeartbeats(LocalDateTime.now());
    }

    public TaskFlowDetail getTaskFlow(String taskId) {
        LocalDateTime now = LocalDateTime.now();
        Map<String, Object> task = taskQueryRepositoryService.findTaskFlowRow("task", "id", taskId);
        if (task.isEmpty()) {
            return null;
        }

        Map<String, Object> videoInfo = taskQueryRepositoryService.findTaskFlowRow("video_info", "task_id", taskId);
        enrichSourceMetadata(videoInfo);
        List<TaskFlowDetail.TaskFlowAsset> minioObjects = listTaskAssets(taskId);
        List<TaskFlowDetail.TaskFlowStage> stages = new ArrayList<>();
        for (StageDefinition definition : STAGES) {
            stages.add(flowStage(taskId, definition, task, videoInfo, minioObjects, now));
        }
        return new TaskFlowDetail(task, videoInfo, stages, minioObjects, now);
    }

    public TaskProgressDetail getTaskProgress(String taskId) {
        return taskQueryRepositoryService.findTaskProgress(taskId, LocalDateTime.now());
    }

    private void enrichSourceMetadata(Map<String, Object> videoInfo) {
        Object submitterVideoId = videoInfo.get("submitter_video_id");
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
        videoInfo.put("title", source.get("title"));
        videoInfo.put("source_description", source.get("description"));
        videoInfo.put("source_uploader", source.get("uploader"));
        videoInfo.put("source_webpage_url", source.get("webpage_url"));
        videoInfo.put("source_tags_json", source.get("tags"));
        videoInfo.put("source_duration_seconds", source.get("duration"));
    }

    public List<WhisperWordTimestamp> whisperWordTimestamps(String taskId) {
        return whisperProcessingRepositoryService.listWhisperWordTimestamps(taskId);
    }

    public WhisperProcessingDetail whisperProcessing(String taskId) {
        return whisperProcessingRepositoryService.findWhisperProcessing(taskId);
    }

    public SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText) {
        return speakerSegmentRepositoryService.updateSpeakerSegmentDstText(taskId, segmentId, dstText);
    }

    public FailedUploadSubmissionList failedUploadSubmissions(String platform) {
        return uploadSubmissionRepositoryService.listFailedUploadSubmissions(platform);
    }

    @Transactional
    public UploadSubmissionRetryResult retryUploadSubmissions(String platform, List<Long> ids) {
        return uploadSubmissionRepositoryService.retryFailedUploadSubmissions(platform, ids);
    }

    public DownloaderFailureList failedTasks() {
        return taskLifecycleRepositoryService.listFailedTasks();
    }

    public DownloaderRollbackResult rollbackDownloaderFailures(List<Long> submissionIds) throws IOException {
        return rollbackFailedTasks(submissionIds);
    }

    public DownloaderRollbackResult rollbackFailedTasks(List<Long> submissionIds) throws IOException {
        List<Long> normalizedIds = submissionIds == null ? List.of() : submissionIds.stream()
                .filter(id -> id != null && id > 0)
                .distinct()
                .toList();
        if (normalizedIds.isEmpty()) {
            throw new IllegalArgumentException("No failed task selected.");
        }
        DownloaderFailureList failures = taskLifecycleRepositoryService.listFailedTasks();
        Map<Long, String> taskIdsBySubmission = failures.rows().stream()
                .filter(row -> normalizedIds.contains(row.submissionId()))
                .collect(java.util.stream.Collectors.toMap(
                        DownloaderFailure::submissionId,
                        DownloaderFailure::taskId
                ));
        if (taskIdsBySubmission.size() != normalizedIds.size()) {
            throw new IllegalArgumentException("Some selected failed tasks no longer exist or are not rollbackable.");
        }

        int deletedObjects = 0;
        for (Long submissionId : normalizedIds) {
            deletedObjects += deleteTaskObjects(taskIdsBySubmission.get(submissionId));
        }
        DownloaderRollbackDatabaseResult databaseResult =
                taskLifecycleRepositoryService.rollbackFailedTasks(normalizedIds);
        return new DownloaderRollbackResult(
                databaseResult.restoredSubmissionCount(),
                databaseResult.deletedDatabaseRows(),
                deletedObjects
        );
    }

    public UploadBackfillCandidateList uploadBackfillCandidates(String platform, String accountKey, String type) {
        return uploadSubmissionRepositoryService.listUploadBackfillCandidates(platform, accountKey, type);
    }

    @Transactional
    public UploadBackfillRegisterResult registerUploadBackfill(String platform, String accountKey, String type, List<String> taskIds) {
        return uploadSubmissionRepositoryService.registerUploadBackfill(platform, accountKey, type, taskIds);
    }

    public SubmitterAuthorType authorType(String author) {
        return submitterAuthorRepositoryService.findSubmitterAuthorType(author);
    }

    public List<SubmitterAuthorType> authorTypes() {
        return submitterAuthorRepositoryService.listSubmitterAuthorTypes();
    }

    public List<DistributorTaskType> distributorTaskTypes() {
        return submitterAuthorRepositoryService.listDistributorTaskTypes();
    }

    public SubmitterAuthorType saveAuthorType(
            String author,
            String type,
            String taskType,
            Boolean hasBackgroundAudio,
            String sourceLanguage,
            String targetLanguage,
            Boolean resetCover,
            String coverOrientation,
            Boolean fetchNewVideos
    ) {
        return submitterAuthorRepositoryService.saveSubmitterAuthorType(
                author,
                type,
                taskType,
                hasBackgroundAudio,
                sourceLanguage,
                targetLanguage,
                resetCover,
                coverOrientation,
                fetchNewVideos
        );
    }

    public SubmitterAuthorDeleteResult deleteAuthorType(String author) {
        return submitterAuthorRepositoryService.deleteSubmitterAuthorType(author);
    }

    @Transactional
    public boolean markTaskReady(String taskId) {
        return taskLifecycleRepositoryService.markTaskReady(taskId);
    }

    @Transactional
    public TaskRestartResult restartTask(String taskId) throws IOException {
        String status = taskLifecycleRepositoryService.findTaskStatus(taskId);
        if (status == null) {
            return null;
        }
        if ("running".equals(status) || taskLifecycleRepositoryService.hasRunningStage(taskId)) {
            throw new IllegalStateException("Task is running. Stop the worker or wait for it to finish before restarting.");
        }

        int deletedObjects = deleteTaskObjects(taskId);
        taskLifecycleRepositoryService.resetTaskRowsForDownloader(taskId);
        return new TaskRestartResult("ready", deletedObjects);
    }

    @Transactional
    public TaskDeleteResult deleteTask(String taskId) throws IOException {
        String status = taskLifecycleRepositoryService.findTaskStatus(taskId);
        if (status == null) {
            return null;
        }
        if ("running".equals(status)) {
            throw new IllegalStateException("Task is running. Stop the worker or wait for it to finish before deleting.");
        }

        int deletedObjects = deleteTaskObjects(taskId);
        int deletedRows = taskLifecycleRepositoryService.deleteTaskRows(taskId);
        return new TaskDeleteResult("deleted", deletedRows, deletedObjects);
    }

    @Transactional
    public TaskStopResult stopTask(String taskId) {
        return taskLifecycleRepositoryService.stopTask(taskId);
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
        Map<String, Object> stageRow = taskQueryRepositoryService.findTaskFlowRow(table, "task_id", taskId);
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
            case "downloader" -> addLimitedTable(tables, "downloader_detail", taskId, "task_id", "kind, id");
            case "whisper" -> {
                addLimitedTable(tables, "asr_segment", taskId, "task_id", "item_index, id");
                addLimitedTable(tables, "whisper_word_timestamp", taskId, "task_id", "segment_index, word_index, id");
            }
            case "translator" -> {
                addLimitedTable(tables, "translator_api_task", taskId, "task_id", "id");
                addLimitedTable(tables, "translator-chunk", taskId, "task_id", "chunk_index, row_order, id");
                addLimitedTable(tables, "translator_segment", taskId, "task_id", "item_index");
            }
            case "speaker" -> addLimitedTable(tables, "speaker_segment", taskId, "task_id", "item_index, id");
            case "publisher" -> {
                addLimitedTable(tables, "publisher_jobs", taskId, "task_id", "job_order, id");
                addLimitedTable(tables, "publisher_result", taskId, "task_id", "task_id");
                addLimitedTable(tables, "product_narration", taskId, "task_id", "id");
                addLimitedTable(tables, "product_narration_sentence", taskId, "task_id", "line_index, id");
            }
            case "asseter" -> {
                addLimitedTable(tables, "asseter_jobs", taskId, "task_id", "id");
                addLimitedTable(tables, "assets", taskId, "task_id", "id");
            }
            case "uploader" -> {
                addLimitedTable(tables, "uploader", taskId, "task_id", "task_id");
                UPLOADER_TASK_TABLES.forEach((platform, table) -> addUploaderTaskTable(tables, platform, table, taskId));
            }
            default -> {
            }
        }
        return tables;
    }

    private void addUploaderTaskTable(List<TaskFlowDetail.TaskFlowTable> tables, String platform, String table, String taskId) {
        if (!taskQueryRepositoryService.tableExists(table)) {
            return;
        }
        List<Map<String, Object>> rows = taskQueryRepositoryService.listTaskFlowRows(table, "task_id", taskId, "account_key, id", CHILD_ROW_LIMIT + 1);
        boolean truncated = rows.size() > CHILD_ROW_LIMIT;
        if (truncated) {
            rows = rows.subList(0, CHILD_ROW_LIMIT);
        }
        if (rows.isEmpty()) {
            return;
        }
        List<Map<String, Object>> decorated = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            java.util.LinkedHashMap<String, Object> decoratedRow = new java.util.LinkedHashMap<>(row);
            decoratedRow.put("platform", platform);
            decorated.add(decoratedRow);
        }
        tables.add(new TaskFlowDetail.TaskFlowTable(table, decorated, truncated));
    }

    private void addLimitedTable(List<TaskFlowDetail.TaskFlowTable> tables, String table, String id, String idColumn, String orderBy) {
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

    private static String minioPrefix(String taskId) {
        String clean = text(taskId).replaceFirst("^/+", "").replaceFirst("/+$", "");
        if (clean.isBlank()) {
            throw new IllegalArgumentException("Missing taskId");
        }
        return clean + "/";
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

    public record DownloaderFailure(
            long submissionId,
            String taskId,
            String title,
            String type,
            String errorMessage,
            LocalDateTime completedAt,
            String sourceUrl
    ) {
    }

    public record DownloaderFailureList(int count, List<DownloaderFailure> rows) {
    }

    public record DownloaderRollbackRequest(List<Long> submissionIds) {
    }

    public record DownloaderRollbackDatabaseResult(int restoredSubmissionCount, int deletedDatabaseRows) {
    }

    public record DownloaderRollbackResult(
            int restoredSubmissionCount,
            int deletedDatabaseRows,
            int deletedMinioObjects
    ) {
    }

    public record UploadBackfillCandidate(
            String taskId,
            String title,
            String coverUrl,
            String finalVideoUrl,
            List<String> uploadedPlatforms,
            LocalDateTime completedAt,
            boolean selectable,
            String blockedReason
    ) {
    }

    public record UploadBackfillCandidateList(
            String platform,
            String accountKey,
            String type,
            int count,
            List<UploadBackfillCandidate> rows
    ) {
    }

    public record UploadBackfillRegisterRequest(String platform, String accountKey, String type, List<String> taskIds) {
    }

    public record UploadBackfillRegisterResult(
            String platform,
            String accountKey,
            String type,
            int registeredCount,
            int skippedCount,
            int uploaderTaskCount,
            int taskCount
    ) {
    }

    public record SubmitterAuthorType(
            String author,
            String type,
            String taskType,
            boolean hasBackgroundAudio,
            String sourceLanguage,
            String targetLanguage,
            boolean resetCover,
            String coverOrientation,
            boolean fetchNewVideos,
            int updatedSubmissionRows,
            int updatedVideoInfoRows
    ) {
    }

    public record DistributorTaskType(String taskType, String name, String description) {
    }

    public record SubmitterAuthorTypeUpdateRequest(
            String author,
            String type,
            String taskType,
            Boolean hasBackgroundAudio,
            String sourceLanguage,
            String targetLanguage,
            Boolean resetCover,
            String coverOrientation,
            Boolean fetchNewVideos
    ) {
    }

    public record SubmitterAuthorDeleteResult(String status, String author, int deletedAuthorRows, int deletedVideoRows) {
    }
}
