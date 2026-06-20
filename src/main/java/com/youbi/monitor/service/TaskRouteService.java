package com.youbi.monitor.service;

import com.youbi.monitor.model.RouteNode;
import com.youbi.monitor.repository.MonitorRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class TaskRouteService {
    private static final Logger log = LoggerFactory.getLogger(TaskRouteService.class);
    private static final Map<String, List<Step>> FALLBACKS = Map.of(
            "repost", steps("downloader", "publisher", "uploader"),
            "subtitle", steps("downloader", "publisher", "demucs", "whisper", "translator", "combiner", "uploader"),
            "dubbing", steps("downloader", "publisher", "demucs", "whisper", "translator", "speaker", "combiner", "uploader"),
            "narration", List.of(
                    new Step("publisher", "main"), new Step("speaker", "main"),
                    new Step("combiner", "audio_merge"), new Step("whisper", "main"),
                    new Step("asseter", "main"), new Step("combiner", "video_render"),
                    new Step("uploader", "main")
            ),
            "asmr", List.of(new Step("downloader", "main"), new Step("publisher", "main"), new Step("combiner", "asmr"))
    );

    private final MonitorRepository repository;
    private final StageRegistry registry;

    public TaskRouteService(MonitorRepository repository, StageRegistry registry) {
        this.repository = repository;
        this.registry = registry;
    }

    @PostConstruct
    void validateConfiguredStages() {
        try {
            for (String stage : repository.queryForList("SELECT DISTINCT stage_name FROM distributor_type_stages", String.class)) {
                registry.require(stage);
            }
        } catch (RuntimeException exc) {
            log.error("Unsafe distributor route configuration detected; lifecycle restart will fail closed", exc);
        }
    }

    public List<RouteNode> routeForTask(String taskId) {
        List<TaskProfile> profiles = repository.query("""
                SELECT task_type, has_background_audio
                FROM video_info
                WHERE task_id = ?
                """, (rs, rowNum) -> new TaskProfile(
                rs.getString("task_type"),
                rs.getObject("has_background_audio") == null || rs.getBoolean("has_background_audio")
        ), taskId);
        if (profiles.isEmpty() || profiles.get(0).taskType() == null || profiles.get(0).taskType().isBlank()) {
            throw new IllegalStateException("Task has no distributor task type: " + taskId);
        }
        TaskProfile profile = profiles.get(0);
        List<Step> configured = repository.query("""
                SELECT stage_name, sub_stage
                FROM distributor_type_stages
                WHERE task_type = ?
                ORDER BY stage_order
                """, (rs, rowNum) -> new Step(rs.getString("stage_name"), normalizeSubStage(rs.getString("sub_stage"))), profile.taskType());
        if (configured.isEmpty()) {
            configured = FALLBACKS.get(profile.taskType());
            if (configured == null) {
                throw new IllegalStateException("No distributor route configured for task type: " + profile.taskType());
            }
            log.warn("Using legacy distributor route fallback for taskId={} taskType={}", taskId, profile.taskType());
        }

        List<RouteNode> route = new ArrayList<>();
        for (Step step : configured) {
            if (!profile.hasBackgroundAudio() && "demucs".equals(step.stage())) {
                continue;
            }
            StageRegistry.StagePolicy policy = registry.require(step.stage());
            String subStage = normalizeSubStage(step.subStage());
            route.add(new RouteNode(
                    step.stage() + ":" + subStage,
                    step.stage(),
                    subStage,
                    label(policy.label(), step.stage(), subStage),
                    route.size() + 1,
                    policy.tableName()
            ));
        }
        if (route.isEmpty()) {
            throw new IllegalStateException("Distributor route is empty for task: " + taskId);
        }
        return List.copyOf(route);
    }

    private static String label(String defaultLabel, String stage, String subStage) {
        if (!"combiner".equals(stage)) return defaultLabel;
        return switch (subStage) {
            case "audio_merge" -> "音频合并";
            case "video_render" -> "视频渲染";
            case "asmr" -> "ASMR 合成";
            default -> defaultLabel;
        };
    }

    private static String normalizeSubStage(String value) {
        return value == null || value.isBlank() ? "main" : value.trim();
    }

    private static List<Step> steps(String... stages) {
        List<Step> result = new ArrayList<>();
        for (String stage : stages) result.add(new Step(stage, "main"));
        return List.copyOf(result);
    }

    private record TaskProfile(String taskType, boolean hasBackgroundAudio) {
    }

    private record Step(String stage, String subStage) {
    }
}
