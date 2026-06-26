package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.RouteEdge;
import com.youbi.monitor.model.StageNode;
import com.youbi.monitor.model.TaskProgressRouteNode;
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
        Map<String, Object> profile = repository.query("""
                SELECT task_type, has_background_audio, narration_input_mode, has_native_subtitle
                FROM video_info
                WHERE task_id = ?
                """, (rs, rowNum) -> Map.<String, Object>of(
                "taskType", rs.getString("task_type") == null ? "" : rs.getString("task_type"),
                "hasBackgroundAudio", rs.getObject("has_background_audio") == null || rs.getBoolean("has_background_audio"),
                "narrationInputMode", rs.getString("narration_input_mode") == null ? "" : rs.getString("narration_input_mode"),
                "hasNativeSubtitleKnown", rs.getObject("has_native_subtitle") != null,
                "hasNativeSubtitle", rs.getObject("has_native_subtitle") != null && rs.getBoolean("has_native_subtitle")
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
        if (configured.isEmpty()) {
            return legacyRouteGraph(stageNodes);
        }

        Set<String> activeIds = activeRouteIds(taskType, profile, configured);
        List<RouteEdge> edges = routeEdges(taskType, configured, activeIds);
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

    private Set<String> activeRouteIds(String taskType, Map<String, Object> profile, List<RouteConfigNode> configured) {
        boolean hasBackgroundAudio = Boolean.TRUE.equals(profile.get("hasBackgroundAudio"));
        String narrationInputMode = String.valueOf(profile.getOrDefault("narrationInputMode", ""));
        boolean hasNativeSubtitleKnown = Boolean.TRUE.equals(profile.get("hasNativeSubtitleKnown"));
        boolean hasNativeSubtitle = Boolean.TRUE.equals(profile.get("hasNativeSubtitle"));
        return configured.stream()
                .filter(node -> routeNodeEnabled(taskType, hasBackgroundAudio, narrationInputMode, hasNativeSubtitleKnown, hasNativeSubtitle, node))
                .map(RouteConfigNode::id)
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
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

    private static boolean routeNodeEnabled(String taskType, boolean hasBackgroundAudio, String narrationInputMode,
                                            boolean hasNativeSubtitleKnown, boolean hasNativeSubtitle,
                                            RouteConfigNode node) {
        if (!hasBackgroundAudio && "demucs".equals(node.stage())) {
            return false;
        }
        if (!"narration".equals(taskType)) {
            return true;
        }
        if ("prepared_text".equals(narrationInputMode)) {
            return !(("downloader".equals(node.stage()) && "metadata".equals(node.subStage()))
                    || ("whisper".equals(node.stage()) && "source_transcription".equals(node.subStage()))
                    || ("publisher".equals(node.stage()) && "script_generation".equals(node.subStage())));
        }
        return !("submission".equals(narrationInputMode)
                && hasNativeSubtitleKnown
                && hasNativeSubtitle
                && "whisper".equals(node.stage())
                && "source_transcription".equals(node.subStage()));
    }

    private Map<String, PhysicalStageState> loadPhysicalSubStageStates(String taskId, LocalDateTime now) {
        Map<String, PhysicalStageState> states = new HashMap<>();
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
            if (index > 0) {
                edges.add(new RouteEdge(routeNodes.get(index - 1).id(), id));
            }
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
            case "publisher:script_generation" -> "文案生成";
            case "publisher:publish_metadata" -> "发布准备";
            case "downloader:metadata" -> "元数据下载";
            case "whisper:source_transcription" -> "源语音识别";
            case "asseter:image_composition" -> "图片素材";
            case "asseter:audio_visualization" -> "音频素材";
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
