package com.youbi.monitor.repository.impl;

import com.youbi.monitor.dto.DeviceHeartbeat;
import com.youbi.monitor.dto.ServiceHeartbeat;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.model.StageError;
import com.youbi.monitor.model.RouteEdge;
import com.youbi.monitor.model.TaskProgressRouteNode;
import com.youbi.monitor.model.TaskMonitorItem;
import com.youbi.monitor.model.TaskMonitorSummary;
import com.youbi.monitor.model.TaskProgressDetail;
import com.youbi.monitor.model.UploadPlatformStatus;
import com.youbi.monitor.repository.IMonitorTaskQueryRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MonitorTaskQueryRepositoryServiceImpl extends MonitorRepositorySqlSupport implements IMonitorTaskQueryRepositoryService {
    private final Map<String, List<String>> tableColumnsCache = new ConcurrentHashMap<>();
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
              vi.minio_storage_bytes,
              vi.minio_storage_object_count,
              vi.minio_storage_updated_at,
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
              u.bilibili_upload_status,
              u.douyin_upload_status,
              u.xiaohongshu_upload_status,
              u.shipinhao_upload_status,
              u.kuaishou_upload_status,
              u.jinritoutiao_upload_status,
              u.bilibili_upload_uid,
              u.bilibili_upload_account_name,
              u.error_message uploader_error
            FROM task t
            LEFT JOIN downloader d ON d.task_id = t.id
            LEFT JOIN demucs de ON de.task_id = t.id
            LEFT JOIN whisper w ON w.task_id = t.id
            LEFT JOIN translator tr ON tr.task_id = t.id
            LEFT JOIN speaker sp ON sp.task_id = t.id
            __ASSETER_JOIN__
            LEFT JOIN combiner m ON m.task_id = t.id
            LEFT JOIN publisher pub ON pub.task_id = t.id
            LEFT JOIN publisher_result pr ON pr.task_id = t.id
            LEFT JOIN uploader u ON u.task_id = t.id
            LEFT JOIN video_info vi ON vi.task_id = t.id
            LEFT JOIN submitter_video sv ON sv.id = vi.submitter_video_id
            __DOWNLOADER_PROGRESS_JOIN__
            LEFT JOIN (
              SELECT task_id, COUNT(*) fixed_count
              FROM asr_segment
              __ASR_TASK_FILTER__
              GROUP BY task_id
            ) fa ON fa.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(*) translated_count
              FROM translator_segment
              __TRANSLATOR_SEGMENT_TASK_FILTER__
              GROUP BY task_id
            ) ts ON ts.task_id = t.id
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
                FROM `translator-chunk` ch
                LEFT JOIN translator_segment s ON s.task_id = ch.task_id AND s.item_index = ch.item_index
                WHERE __TRANSLATOR_CHUNK_TASK_FILTER__ ch.row_role = 'normal'
                GROUP BY ch.task_id, ch.chunk_index
              ) chunk_progress
              GROUP BY chunk_progress.task_id
            ) tc ON tc.task_id = t.id
            LEFT JOIN (
              SELECT task_id, COUNT(DISTINCT item_index) translator_failed_count
              FROM translator_api_task
              WHERE __TRANSLATOR_FAILURE_TASK_FILTER__ status = 'failed' AND request_key LIKE 'chunk:%'
              GROUP BY task_id
            ) tf ON tf.task_id = t.id
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
              vi.minio_storage_bytes,
              vi.minio_storage_object_count,
              vi.minio_storage_updated_at,
              vi.type task_type,
              t.status,
              t.current_stage,
              t.created_at,
              t.started_at,
              t.completed_at,
              t.error_message
            FROM task t
            LEFT JOIN video_info vi ON vi.task_id = t.id
            LEFT JOIN submitter_video sv ON sv.id = vi.submitter_video_id
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
        super(repository);
    }

    @Override
    public void ensureMonitorSchema() {
        dropColumnIfExists("task", "operator");
        repository.update("""
                CREATE TABLE IF NOT EXISTS publisher (
                    task_id VARCHAR(64) PRIMARY KEY,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    started_at DATETIME NULL,
                    completed_at DATETIME NULL,
                    error_message TEXT NULL,
                    `operator` VARCHAR(128) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
        ensureVideoInfoColumn("minio_storage_bytes", "BIGINT UNSIGNED NULL");
        ensureVideoInfoColumn("minio_storage_object_count", "INT UNSIGNED NULL");
        ensureVideoInfoColumn("minio_storage_updated_at", "DATETIME NULL");
    }

    private void ensurePublisherColumn(String column, String definition) {
        if (!columnExists("publisher_result", column)) {
            repository.update("ALTER TABLE publisher_result ADD COLUMN " + quotedIdentifier(column) + " " + definition);
            invalidateSchemaCapability("publisher_result", column);
        }
    }

    @Override
    public List<TaskMonitorSummary> listTaskMonitorItems(LocalDateTime now, int limit, int offset, String status, String type, String stage, String taskId, String sort) {
        SqlFilter filter = taskMonitorFilter(status, type, stage, taskId);
        List<Object> args = new ArrayList<>(filter.args());
        args.add(limit);
        args.add(offset);
        String sql = MONITOR_SUMMARY_SQL
                .replace("__TASK_MONITOR_WHERE__", filter.clause())
                .replace("__TASK_MONITOR_ORDER_BY__", monitorOrderBy(sort));
        return repository.query(sql, new TaskSummaryRowMapper(now), args.toArray());
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
        for (int i = 0; i < 5; i++) {
            args.add(taskId);
        }
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
        RouteGraph graph = routeGraph(taskId, nodes, now);
        return new TaskProgressDetail(taskId, row.distributorStages(), nodes, graph.nodes(), graph.edges());
    }

    private RouteGraph routeGraph(String taskId, List<StageNode> stageNodes, LocalDateTime now) {
        Map<String, Object> profile = repository.query("""
                SELECT task_type, has_background_audio
                FROM video_info
                WHERE task_id = ?
                """, (rs, rowNum) -> Map.<String, Object>of(
                "taskType", rs.getString("task_type") == null ? "" : rs.getString("task_type"),
                "hasBackgroundAudio", rs.getObject("has_background_audio") == null || rs.getBoolean("has_background_audio")
        ), taskId).stream().findFirst().orElse(Map.of());
        String taskType = String.valueOf(profile.getOrDefault("taskType", ""));
        if (taskType.isBlank() || !tableExists("distributor_type_stages")) {
            return legacyRouteGraph(stageNodes);
        }

        List<RouteConfigNode> configured = repository.query("""
                SELECT stage_name, sub_stage, stage_order
                FROM distributor_type_stages
                WHERE task_type = ?
                ORDER BY stage_order, stage_name, sub_stage
                """, (rs, rowNum) -> new RouteConfigNode(
                rs.getString("stage_name"), rs.getString("sub_stage"), rs.getInt("stage_order")
        ), taskType);
        if (configured.isEmpty()) return legacyRouteGraph(stageNodes);

        boolean hasBackgroundAudio = Boolean.TRUE.equals(profile.get("hasBackgroundAudio"));
        Set<String> configuredIds = configured.stream().map(RouteConfigNode::id).collect(java.util.stream.Collectors.toSet());
        Map<String, Set<String>> parents = new LinkedHashMap<>();
        configured.forEach(node -> parents.put(node.id(), new java.util.LinkedHashSet<>()));
        if (tableExists("distributor_type_stage_dependencies")) {
            repository.query("""
                    SELECT stage_name, sub_stage, depends_on_stage_name, depends_on_sub_stage
                    FROM distributor_type_stage_dependencies
                    WHERE task_type = ?
                    """, (rs, rowNum) -> {
                String child = routeId(rs.getString("stage_name"), rs.getString("sub_stage"));
                String parent = routeId(rs.getString("depends_on_stage_name"), rs.getString("depends_on_sub_stage"));
                if (configuredIds.contains(child) && configuredIds.contains(parent)) parents.get(child).add(parent);
                return child;
            }, taskType);
        }

        Set<String> activeIds = configured.stream()
                .filter(node -> hasBackgroundAudio || !"demucs".equals(node.stage()))
                .map(RouteConfigNode::id)
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
        List<RouteEdge> edges = new ArrayList<>();
        for (RouteConfigNode child : configured) {
            if (!activeIds.contains(child.id())) continue;
            Set<String> resolved = new java.util.LinkedHashSet<>();
            for (String parent : parents.getOrDefault(child.id(), Set.of())) {
                resolveActiveParents(parent, parents, activeIds, resolved, new HashSet<>());
            }
            resolved.forEach(parent -> edges.add(new RouteEdge(parent, child.id())));
        }

        Map<String, StageNode> baseNodes = new HashMap<>();
        stageNodes.forEach(node -> baseNodes.put(node.key(), node));
        Map<String, PhysicalStageState> physicalStates = loadPhysicalSubStageStates(taskId, now);
        RouteLogState routeLogState = loadRouteLogState(taskId);
        List<TaskProgressRouteNode> routeNodes = configured.stream()
                .filter(node -> activeIds.contains(node.id()))
                .map(node -> toRouteNode(node, baseNodes.get(node.stage()), physicalStates.get(node.id()), routeLogState, now))
                .toList();
        return new RouteGraph(routeNodes, edges);
    }

    private static void resolveActiveParents(String node, Map<String, Set<String>> parents, Set<String> active,
                                             Set<String> resolved, Set<String> visiting) {
        if (!visiting.add(node)) return;
        if (active.contains(node)) {
            resolved.add(node);
            return;
        }
        for (String parent : parents.getOrDefault(node, Set.of())) {
            resolveActiveParents(parent, parents, active, resolved, visiting);
        }
    }

    private Map<String, PhysicalStageState> loadPhysicalSubStageStates(String taskId, LocalDateTime now) {
        Map<String, PhysicalStageState> states = new HashMap<>();
        for (String stage : List.of("publisher", "combiner")) {
            if (!tableExists(stage) || !columnExists(stage, "sub_stage")) continue;
            repository.query("SELECT sub_stage, status, started_at, completed_at, error_message FROM " + quotedIdentifier(stage) + " WHERE task_id = ?",
                    (rs, rowNum) -> {
                        LocalDateTime startedAt = timestamp(rs, "started_at");
                        LocalDateTime completedAt = timestamp(rs, "completed_at");
                        states.put(routeId(stage, rs.getString("sub_stage")), new PhysicalStageState(
                                rs.getString("status"), startedAt, completedAt,
                                elapsedSeconds(startedAt, completedAt, now), rs.getString("error_message")
                        ));
                        return stage;
                    }, taskId);
        }
        return states;
    }

    private RouteLogState loadRouteLogState(String taskId) {
        if (!tableExists("distributor_route_log")) return new RouteLogState(Set.of(), Set.of(), Map.of());
        Set<String> completed = new HashSet<>();
        Set<String> released = new HashSet<>();
        Map<String, LocalDateTime> completedAt = new HashMap<>();
        repository.query("""
                SELECT from_stage, from_sub_stage, to_stage, to_sub_stage, action, created_at
                FROM distributor_route_log
                WHERE task_id = ?
                ORDER BY id
                """, (rs, rowNum) -> {
            String action = rs.getString("action");
            String from = routeId(rs.getString("from_stage"), rs.getString("from_sub_stage"));
            if ("evaluated".equals(action) || "advance".equals(action) || "complete".equals(action)) {
                completed.add(from);
                completedAt.put(from, timestamp(rs, "created_at"));
            }
            String toStage = rs.getString("to_stage");
            if (toStage != null && action != null && (action.startsWith("release:") || "advance".equals(action))) {
                released.add(routeId(toStage, rs.getString("to_sub_stage")));
            }
            return action;
        }, taskId);
        return new RouteLogState(completed, released, completedAt);
    }

    private TaskProgressRouteNode toRouteNode(RouteConfigNode config, StageNode base, PhysicalStageState physical,
                                  RouteLogState logs, LocalDateTime now) {
        boolean completed = logs.completed().contains(config.id());
        String status;
        LocalDateTime startedAt = null;
        LocalDateTime completedAt = null;
        long elapsed = 0;
        String error = null;
        boolean useBase = "main".equals(config.subStage()) && physical == null;
        if (completed) {
            status = "success";
            completedAt = logs.completedAt().get(config.id());
        } else if (physical != null) {
            status = physical.status();
            startedAt = physical.startedAt();
            completedAt = physical.completedAt();
            elapsed = physical.elapsedSeconds();
            error = physical.errorMessage();
        } else if (useBase && base != null) {
            status = base.status();
            startedAt = base.startedAt();
            completedAt = base.completedAt();
            elapsed = base.elapsedSeconds();
            error = base.errorMessage();
        } else {
            status = logs.released().contains(config.id()) ? "ready" : "pending";
        }
        return new TaskProgressRouteNode(
                config.id(), config.stage(), config.subStage(), routeLabel(config.stage(), config.subStage()), config.order(), status,
                startedAt, completedAt, elapsed,
                useBase && base != null ? base.completedCount() : null,
                useBase && base != null ? base.failedCount() : null,
                useBase && base != null ? base.totalCount() : null,
                useBase && base != null ? base.progressPercent() : null,
                error,
                useBase && base != null ? base.childErrorMessage() : null,
                useBase && base != null ? base.platformStatuses() : List.of(),
                useBase && base != null ? base.errors() : List.of(),
                useBase && base != null ? base.errorCount() : 0,
                useBase && base != null && base.errorsTruncated()
        );
    }

    private RouteGraph legacyRouteGraph(List<StageNode> nodes) {
        List<TaskProgressRouteNode> routeNodes = new ArrayList<>();
        List<RouteEdge> edges = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            StageNode node = nodes.get(index);
            String id = routeId(node.key(), "main");
            routeNodes.add(new TaskProgressRouteNode(id, node.key(), "main", node.label(), index + 1, node.status(), node.startedAt(),
                    node.completedAt(), node.elapsedSeconds(), node.completedCount(), node.failedCount(), node.totalCount(),
                    node.progressPercent(), node.errorMessage(), node.childErrorMessage(), node.platformStatuses(), node.errors(),
                    node.errorCount(), node.errorsTruncated()));
            if (index > 0) edges.add(new RouteEdge(routeNodes.get(index - 1).id(), id));
        }
        return new RouteGraph(routeNodes, edges);
    }

    private static String routeId(String stage, String subStage) {
        return stage + ":" + (subStage == null || subStage.isBlank() ? "main" : subStage);
    }

    private static String routeLabel(String stage, String subStage) {
        String id = routeId(stage, subStage);
        return switch (id) {
            case "publisher:segment_plan" -> "文案分段";
            case "publisher:image_generation" -> "图片生成";
            case "combiner:audio_merge" -> "音频合并";
            case "combiner:video_render" -> "视频渲染";
            case "combiner:asmr" -> "ASMR 合成";
            default -> switch (stage) {
                case "downloader" -> "下载";
                case "publisher" -> "发布准备";
                case "demucs" -> "人声分离";
                case "whisper" -> "语音识别";
                case "translator" -> "翻译";
                case "speaker" -> "配音";
                case "asseter" -> "素材加工";
                case "combiner" -> "音视频合成";
                case "uploader" -> "上传";
                default -> stage;
            };
        };
    }

    private record RouteConfigNode(String stage, String subStage, int order) {
        private String id() { return routeId(stage, subStage); }
    }
    private record PhysicalStageState(String status, LocalDateTime startedAt, LocalDateTime completedAt,
                                      long elapsedSeconds, String errorMessage) {}
    private record RouteLogState(Set<String> completed, Set<String> released, Map<String, LocalDateTime> completedAt) {}
    private record RouteGraph(List<TaskProgressRouteNode> nodes, List<RouteEdge> edges) {}

    private List<StageNode> withStructuredErrors(String taskId, List<StageNode> nodes) {
        List<StageNode> enriched = new ArrayList<>(nodes.size());
        for (StageNode node : nodes) {
            if ("translator".equals(node.key())) {
                enriched.add(withErrors(node, translatorErrors(taskId), errorCount("translator_api_task", taskId)));
            } else if ("speaker".equals(node.key())) {
                enriched.add(withErrors(node, speakerErrors(taskId), errorCount("speaker_segment", taskId)));
            } else {
                enriched.add(node);
            }
        }
        return enriched;
    }

    private List<StageError> translatorErrors(String taskId) {
        return repository.query("""
                SELECT item_index, COALESCE(NULLIF(error_message, ''), status) message
                FROM translator_api_task
                WHERE task_id = ? AND status = 'failed'
                ORDER BY id DESC
                LIMIT 20
                """, (rs, rowNum) -> new StageError(integerOrNull(rs, "item_index"), rs.getString("message")), taskId);
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
                .replace("__ASR_TASK_FILTER__", "WHERE task_id = ?")
                .replace("__TRANSLATOR_SEGMENT_TASK_FILTER__", "WHERE task_id = ?")
                .replace("__TRANSLATOR_CHUNK_TASK_FILTER__", "ch.task_id = ? AND")
                .replace("__TRANSLATOR_FAILURE_TASK_FILTER__", "task_id = ? AND")
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
        if (tableExists("asseter")) {
            asseterSelect = """
                    a.status asseter_status,
                    a.started_at asseter_started_at,
                    a.completed_at asseter_completed_at,
                    a.error_message asseter_error,
                    """;
            asseterJoin = "LEFT JOIN asseter a ON a.task_id = t.id";
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
                .replace("__DISTRIBUTOR_STAGES_SELECT__", distributorStagesSelect);
    }

    private static String monitorOrderBy(String sort) {
        String normalized = text(sort);
        if ("minio_storage_desc".equalsIgnoreCase(normalized)) {
            return "ORDER BY COALESCE(vi.minio_storage_bytes, 0) DESC, t.created_at DESC";
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
                    rs.getString("bilibili_upload_uid"),
                    rs.getString("bilibili_upload_account_name"),
                    rs.getString("error_message"),
                    distributorStages(rs),
                    nodes
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

    private static class TaskSummaryRowMapper implements RowMapper<TaskMonitorSummary> {
        private final LocalDateTime now;

        private TaskSummaryRowMapper(LocalDateTime now) {
            this.now = now;
        }

        @Override
        public TaskMonitorSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            LocalDateTime startedAt = timestamp(rs, "started_at");
            LocalDateTime completedAt = timestamp(rs, "completed_at");
            return new TaskMonitorSummary(
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
                    startedAt,
                    completedAt,
                    elapsedSeconds(startedAt, completedAt, now),
                    rs.getString("error_message")
            );
        }

        private static Double doubleOrNull(ResultSet rs, String column) throws SQLException {
            double value = rs.getDouble(column);
            return rs.wasNull() ? null : value;
        }

        private static Integer integerOrNull(ResultSet rs, String column) throws SQLException {
            int value = rs.getInt(column);
            return rs.wasNull() ? null : value;
        }
    }

    private record SqlFilter(String clause, List<Object> args) {
    }
}
