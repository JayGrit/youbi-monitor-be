package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.RouteEdge;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.model.TaskProgressRouteNode;
import com.youbi.monitor.model.JobSummary;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
class TaskProgressRouteGraphBuilder extends MonitorRepositorySqlSupport {

    TaskProgressRouteGraphBuilder(MonitorRepository repository) {
        super(repository);
    }

    RouteGraph build(String taskId, List<StageNode> stageNodes, LocalDateTime now) {
        return build(taskId, stageNodes, now, false);
    }

    RouteGraph build(String taskId, List<StageNode> stageNodes, LocalDateTime now, boolean includeJobSummaries) {
        RouteGraph snapshot = snapshotRouteGraph(taskId, stageNodes, now, includeJobSummaries);
        if (snapshot != null) {
            return snapshot;
        }

        Map<String, Object> profile = repository.query("""
                SELECT t.task_type, opts.has_background_audio, opts.has_native_subtitle
                FROM task t
                LEFT JOIN task_options opts ON opts.task_id = t.id
                WHERE t.id = ?
                """, (rs, rowNum) -> Map.<String, Object>of(
                "taskType", rs.getString("task_type") == null ? "" : rs.getString("task_type"),
                "hasBackgroundAudio", rs.getObject("has_background_audio") == null || rs.getBoolean("has_background_audio"),
                "hasNativeSubtitleKnown", rs.getObject("has_native_subtitle") != null,
                "hasNativeSubtitle", rs.getObject("has_native_subtitle") != null && rs.getBoolean("has_native_subtitle")
        ), taskId).stream().findFirst().orElse(Map.of());
        String taskType = routeTaskType(String.valueOf(profile.getOrDefault("taskType", "")));
        if (taskType.isBlank() || !tableExists("distributor_type_stages")) {
            return legacyRouteGraph(stageNodes);
        }

        List<RouteConfigNode> configured = repository.query("""
                SELECT stage_name, sub_stage, stage_order, COALESCE(NULLIF(remark, ''), '') AS label
                FROM distributor_type_stages
                WHERE task_type = ?
                ORDER BY stage_order, stage_name, sub_stage
                """, (rs, rowNum) -> new RouteConfigNode(
                rs.getString("stage_name"), rs.getString("sub_stage"), rs.getInt("stage_order"), rs.getString("label")
        ), taskType);
        if (configured.isEmpty()) {
            return legacyRouteGraph(stageNodes);
        }

        Set<String> activeIds = activeRouteIds(taskType, profile, configured);
        List<RouteEdge> edges = routeEdges(taskType, configured, activeIds);
        Map<String, StageNode> baseNodes = new HashMap<>();
        stageNodes.forEach(node -> baseNodes.put(node.key(), node));
        Map<String, PhysicalStageState> physicalStates = loadPhysicalSubStageStates(taskId, now);
        RouteLogState routeLogState = loadRouteLogState(taskId);
        Map<String, JobSummary> jobSummaries = includeJobSummaries
                ? loadJobSummaries(taskId, configured, activeIds, now)
                : Map.of();
        List<TaskProgressRouteNode> routeNodes = configured.stream()
                .filter(node -> activeIds.contains(node.id()))
                .map(node -> toRouteNode(node, baseNodes.get(node.stage()), physicalStates.get(node.id()),
                        jobSummaries.get(node.id()), routeLogState, now))
                .toList();
        return new RouteGraph(routeNodes, edges);
    }

    private RouteGraph snapshotRouteGraph(String taskId, List<StageNode> stageNodes, LocalDateTime now,
                                          boolean includeJobSummaries) {
        if (!tableExists("distributor_task_route_nodes") || !tableExists("distributor_task_route_edges")) {
            return null;
        }

        List<RouteConfigNode> snapshotNodes = repository.query("""
                SELECT node.stage_name, node.sub_stage, node.stage_order,
                       COALESCE(NULLIF(type_stage.remark, ''), '') AS label
                FROM distributor_task_route_nodes node
                LEFT JOIN distributor_type_stages type_stage
                  ON type_stage.task_type = node.task_type
                 AND type_stage.stage_name = node.stage_name
                 AND type_stage.sub_stage = node.sub_stage
                WHERE node.task_id = ?
                ORDER BY node.stage_order, node.stage_name, node.sub_stage
                """, (rs, rowNum) -> new RouteConfigNode(
                rs.getString("stage_name"), rs.getString("sub_stage"), rs.getInt("stage_order"), rs.getString("label")
        ), taskId);
        if (snapshotNodes.isEmpty()) {
            return null;
        }

        Set<String> activeIds = snapshotNodes.stream()
                .map(RouteConfigNode::id)
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
        List<RouteEdge> edges = repository.query("""
                SELECT stage_name, sub_stage, depends_on_stage_name, depends_on_sub_stage
                FROM distributor_task_route_edges
                WHERE task_id = ?
                ORDER BY stage_name, sub_stage, depends_on_stage_name, depends_on_sub_stage
                """, (rs, rowNum) -> new RouteEdge(
                routeId(rs.getString("depends_on_stage_name"), rs.getString("depends_on_sub_stage")),
                routeId(rs.getString("stage_name"), rs.getString("sub_stage"))
        ), taskId);

        Map<String, StageNode> baseNodes = new HashMap<>();
        stageNodes.forEach(node -> baseNodes.put(node.key(), node));
        Map<String, PhysicalStageState> physicalStates = loadPhysicalSubStageStates(taskId, now);
        RouteLogState routeLogState = loadRouteLogState(taskId);
        Map<String, JobSummary> jobSummaries = includeJobSummaries
                ? loadJobSummaries(taskId, snapshotNodes, activeIds, now)
                : Map.of();
        List<TaskProgressRouteNode> routeNodes = snapshotNodes.stream()
                .map(node -> toRouteNode(node, baseNodes.get(node.stage()), physicalStates.get(node.id()),
                        jobSummaries.get(node.id()), routeLogState, now))
                .toList();
        return new RouteGraph(routeNodes, edges);
    }

    private Set<String> activeRouteIds(String taskType, Map<String, Object> profile, List<RouteConfigNode> configured) {
        boolean hasBackgroundAudio = Boolean.TRUE.equals(profile.get("hasBackgroundAudio"));
        boolean hasNativeSubtitleKnown = Boolean.TRUE.equals(profile.get("hasNativeSubtitleKnown"));
        boolean hasNativeSubtitle = Boolean.TRUE.equals(profile.get("hasNativeSubtitle"));
        return configured.stream()
                .filter(node -> routeNodeEnabled(taskType, hasBackgroundAudio, hasNativeSubtitleKnown, hasNativeSubtitle, node))
                .map(RouteConfigNode::id)
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    private static String routeTaskType(String taskType) {
        String normalized = text(taskType);
        return switch (normalized) {
            case "xb" -> "repost";
            default -> normalized;
        };
    }

    private List<RouteEdge> routeEdges(String taskType, List<RouteConfigNode> configured, Set<String> activeIds) {
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
                if (configuredIds.contains(child) && configuredIds.contains(parent)) {
                    parents.get(child).add(parent);
                }
                return child;
            }, taskType);
        }

        List<RouteEdge> edges = new ArrayList<>();
        for (RouteConfigNode child : configured) {
            if (!activeIds.contains(child.id())) {
                continue;
            }
            Set<String> resolved = new java.util.LinkedHashSet<>();
            for (String parent : parents.getOrDefault(child.id(), Set.of())) {
                resolveActiveParents(parent, parents, activeIds, resolved, new HashSet<>());
            }
            resolved.forEach(parent -> edges.add(new RouteEdge(parent, child.id())));
        }
        return edges;
    }

    private static void resolveActiveParents(String node, Map<String, Set<String>> parents, Set<String> active,
                                             Set<String> resolved, Set<String> visiting) {
        if (!visiting.add(node)) {
            return;
        }
        if (active.contains(node)) {
            resolved.add(node);
            return;
        }
        for (String parent : parents.getOrDefault(node, Set.of())) {
            resolveActiveParents(parent, parents, active, resolved, visiting);
        }
    }

    private static boolean routeNodeEnabled(String taskType, boolean hasBackgroundAudio,
                                            boolean hasNativeSubtitleKnown, boolean hasNativeSubtitle,
                                            RouteConfigNode node) {
        if (!"narration".equals(taskType)) {
            if (!hasBackgroundAudio && "demucs".equals(node.stage())) {
                return false;
            }
            return true;
        }
        if ("demucs".equals(node.stage())) {
            return hasNativeSubtitleKnown
                    && !hasNativeSubtitle
                    && hasBackgroundAudio;
        }
        if ("downloader".equals(node.stage()) && "audio".equals(node.subStage())) {
            return hasNativeSubtitleKnown && !hasNativeSubtitle;
        }
        if ("whisper".equals(node.stage()) && "source_transcription".equals(node.subStage())) {
            return hasNativeSubtitleKnown && !hasNativeSubtitle;
        }
        if ("publisher".equals(node.stage()) && "script_generation".equals(node.subStage())) {
            return hasNativeSubtitleKnown;
        }
        return true;
    }

    private Map<String, PhysicalStageState> loadPhysicalSubStageStates(String taskId, LocalDateTime now) {
        Map<String, PhysicalStageState> states = new HashMap<>();
        if (tableExists("distributor_task_stages")) {
            repository.query("""
                    SELECT stage_name, sub_stage, status, started_at, completed_at, error_message
                    FROM distributor_task_stages
                    WHERE task_id = ? AND sub_stage <> 'main'
                    """, (rs, rowNum) -> {
                LocalDateTime startedAt = timestamp(rs, "started_at");
                LocalDateTime completedAt = timestamp(rs, "completed_at");
                states.put(routeId(rs.getString("stage_name"), rs.getString("sub_stage")), new PhysicalStageState(
                        rs.getString("status"), startedAt, completedAt,
                        elapsedSeconds(startedAt, completedAt, now), rs.getString("error_message")
                ));
                return rs.getString("stage_name");
            }, taskId);
        }
        for (String stage : List.of("downloader", "publisher", "whisper", "asseter", "combiner")) {
            if (!tableExists(stage) || !columnExists(stage, "sub_stage")) {
                continue;
            }
            repository.query("SELECT sub_stage, status, started_at, completed_at, error_message FROM "
                            + quotedIdentifier(stage)
                            + " WHERE task_id = ? AND sub_stage <> 'main'",
                    (rs, rowNum) -> {
                        LocalDateTime startedAt = timestamp(rs, "started_at");
                        LocalDateTime completedAt = timestamp(rs, "completed_at");
                        states.putIfAbsent(routeId(stage, rs.getString("sub_stage")), new PhysicalStageState(
                                rs.getString("status"), startedAt, completedAt,
                                elapsedSeconds(startedAt, completedAt, now), rs.getString("error_message")
                        ));
                        return stage;
                    }, taskId);
        }
        return states;
    }

    private RouteLogState loadRouteLogState(String taskId) {
        if (!tableExists("distributor_route_log")) {
            return new RouteLogState(Set.of(), Set.of(), Map.of());
        }
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

    private Map<String, JobSummary> loadJobSummaries(String taskId, List<RouteConfigNode> configured, Set<String> activeIds,
                                                     LocalDateTime now) {
        Map<String, JobSummary> summaries = new HashMap<>();
        loadSubStageJobSummaries(taskId, now, summaries, "publisher", "publisher_jobs", activeIds);
        loadSubStageJobSummaries(taskId, now, summaries, "asseter", "asseter_jobs", activeIds);
        loadSubStageJobSummaries(taskId, now, summaries, "combiner", "combiner_jobs", activeIds);
        loadMainJobSummary(taskId, now, summaries, "translator", "translator_jobs", routeId("translator", "main"), activeIds);
        loadMainJobSummary(taskId, now, summaries, "uploader", "uploader_task", routeId("uploader", "main"), activeIds);
        loadMainJobSummary(taskId, now, summaries, "uploader", "uploader_task_status", routeId("uploader", "main"), activeIds);
        loadMainJobSummary(taskId, now, summaries, "speaker", "speaker_segment",
                speakerRouteId(configured, activeIds), activeIds);
        return summaries;
    }

    private void loadSubStageJobSummaries(String taskId, LocalDateTime now, Map<String, JobSummary> summaries,
                                          String serviceName, String table, Set<String> activeIds) {
        if (!hasColumns(table, "task_id", "sub_stage", "status", "started_at", "completed_at")) {
            return;
        }
        repository.query("""
                SELECT sub_stage, COUNT(*) total_count,
                       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) completed_count,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) failed_count,
                       SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) running_count,
                       SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) pending_count,
                       SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) ready_count,
                       MIN(started_at) started_at,
                       MAX(completed_at) completed_at
                FROM %s
                WHERE task_id = ?
                GROUP BY sub_stage
                """.formatted(quotedIdentifier(table)), (rs, rowNum) -> {
            String routeId = routeId(serviceName, rs.getString("sub_stage"));
            if (activeIds.contains(routeId)) {
                summaries.put(routeId, jobSummaryFromRow(serviceName, table, now, rs));
            }
            return routeId;
        }, taskId);
    }

    private void loadMainJobSummary(String taskId, LocalDateTime now, Map<String, JobSummary> summaries,
                                    String serviceName, String table, String routeId, Set<String> activeIds) {
        if (routeId == null || !activeIds.contains(routeId)
                || !hasColumns(table, "task_id", "status", "started_at", "completed_at")) {
            return;
        }
        List<JobSummary> rows = repository.query("""
                SELECT COUNT(*) total_count,
                       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) completed_count,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) failed_count,
                       SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) running_count,
                       SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) pending_count,
                       SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) ready_count,
                       MIN(started_at) started_at,
                       MAX(completed_at) completed_at
                FROM %s
                WHERE task_id = ?
                """.formatted(quotedIdentifier(table)), (rs, rowNum) -> jobSummaryFromRow(serviceName, table, now, rs), taskId);
        rows.stream()
                .filter(summary -> summary.totalCount() > 0)
                .findFirst()
                .ifPresent(summary -> summaries.put(routeId, summary));
    }

    private static JobSummary jobSummaryFromRow(String serviceName, String sourceTable, LocalDateTime now,
                                                java.sql.ResultSet rs) throws java.sql.SQLException {
        LocalDateTime startedAt = timestamp(rs, "started_at");
        LocalDateTime completedAt = timestamp(rs, "completed_at");
        return new JobSummary(
                serviceName,
                sourceTable,
                rs.getInt("total_count"),
                rs.getInt("completed_count"),
                rs.getInt("failed_count"),
                rs.getInt("running_count"),
                rs.getInt("pending_count"),
                rs.getInt("ready_count"),
                startedAt,
                completedAt,
                elapsedSeconds(startedAt, completedAt, now)
        );
    }

    private boolean hasColumns(String table, String... columns) {
        if (!tableExists(table)) {
            return false;
        }
        for (String column : columns) {
            if (!columnExists(table, column)) {
                return false;
            }
        }
        return true;
    }

    private static String speakerRouteId(List<RouteConfigNode> configured, Set<String> activeIds) {
        for (String candidate : List.of(routeId("speaker", "narration"), routeId("speaker", "dubbing_multi_segment"), routeId("speaker", "main"))) {
            if (activeIds.contains(candidate)) {
                return candidate;
            }
        }
        return configured.stream()
                .map(RouteConfigNode::id)
                .filter(activeIds::contains)
                .filter(id -> id.startsWith("speaker:"))
                .findFirst()
                .orElse(null);
    }

    private TaskProgressRouteNode toRouteNode(RouteConfigNode config, StageNode base, PhysicalStageState physical,
                                              JobSummary jobSummary,
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
        if (jobSummary != null) {
            if (startedAt == null) {
                startedAt = jobSummary.startedAt();
            }
            if (completedAt == null) {
                completedAt = jobSummary.completedAt();
            }
            if (elapsed == 0) {
                elapsed = jobSummary.elapsedSeconds();
            }
        }
        Integer completedCount = jobSummary != null
                ? Integer.valueOf(jobSummary.completedCount())
                : (useBase && base != null ? base.completedCount() : null);
        Integer failedCount = jobSummary != null
                ? Integer.valueOf(jobSummary.failedCount())
                : (useBase && base != null ? base.failedCount() : null);
        Integer totalCount = jobSummary != null
                ? Integer.valueOf(jobSummary.totalCount())
                : (useBase && base != null ? base.totalCount() : null);
        return new TaskProgressRouteNode(
                config.id(), config.stage(), config.subStage(), routeLabel(config), config.order(), status,
                startedAt, completedAt, elapsed,
                completedCount,
                failedCount,
                totalCount,
                useBase && base != null ? base.progressPercent() : null,
                error,
                useBase && base != null ? base.childErrorMessage() : null,
                jobSummary,
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
                    node.progressPercent(), node.errorMessage(), node.childErrorMessage(), null, node.platformStatuses(), node.errors(),
                    node.errorCount(), node.errorsTruncated()));
            if (index > 0) {
                edges.add(new RouteEdge(routeNodes.get(index - 1).id(), id));
            }
        }
        return new RouteGraph(routeNodes, edges);
    }

    private static String routeId(String stage, String subStage) {
        return stage + ":" + (subStage == null || subStage.isBlank() ? "main" : subStage);
    }

    private static String routeLabel(RouteConfigNode config) {
        String configuredLabel = text(config.label());
        if (!configuredLabel.isBlank()) {
            return configuredLabel;
        }
        return "Unknown";
    }

    private record RouteConfigNode(String stage, String subStage, int order, String label) {
        private String id() {
            return routeId(stage, subStage);
        }
    }

    private record PhysicalStageState(String status, LocalDateTime startedAt, LocalDateTime completedAt,
                                      long elapsedSeconds, String errorMessage) {
    }

    private record RouteLogState(Set<String> completed, Set<String> released, Map<String, LocalDateTime> completedAt) {
    }

    record RouteGraph(List<TaskProgressRouteNode> nodes, List<RouteEdge> edges) {
    }
}
