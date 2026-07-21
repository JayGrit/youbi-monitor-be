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
                    new Step("downloader", "metadata", null), new Step("downloader", "audio", null),
                    new Step("demucs", "main", null), new Step("whisper", "source_transcription", null),
                    new Step("publisher", "script_generation", null), new Step("publisher", "publish_metadata", null),
                    new Step("publisher", "segment_plan", null), new Step("publisher", "image_generation", null),
                    new Step("asseter", "image_composition", null), new Step("speaker", "narration", null),
                    new Step("combiner", "audio_merge", null), new Step("whisper", "main", null),
                    new Step("asseter", "audio_visualization", null), new Step("combiner", "video_render", null),
                    new Step("uploader", "main", null)
            ),
            "asmr", List.of(new Step("downloader", "main", null), new Step("publisher", "main", null), new Step("combiner", "asmr", null))
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
                SELECT t.task_type, opts.has_background_audio, opts.has_native_subtitle
                FROM task t
                LEFT JOIN task_options opts ON opts.task_id = t.id
                WHERE t.id = ?
                """, (rs, rowNum) -> new TaskProfile(
                rs.getString("task_type"),
                rs.getObject("has_background_audio") == null || rs.getBoolean("has_background_audio"),
                rs.getObject("has_native_subtitle") == null ? null : rs.getBoolean("has_native_subtitle")
        ), taskId);
        if (profiles.isEmpty() || profiles.get(0).taskType() == null || profiles.get(0).taskType().isBlank()) {
            throw new IllegalStateException("Task has no distributor task type: " + taskId);
        }
        TaskProfile profile = profiles.get(0);
        List<Step> configured = repository.query("""
                SELECT stage_name, sub_stage, COALESCE(NULLIF(remark, ''), '') AS label
                FROM distributor_type_stages
                WHERE task_type = ?
                ORDER BY stage_order
                """, (rs, rowNum) -> new Step(
                rs.getString("stage_name"),
                normalizeSubStage(rs.getString("sub_stage")),
                rs.getString("label")
        ), profile.taskType());
        if (configured.isEmpty()) {
            configured = FALLBACKS.get(profile.taskType());
            if (configured == null) {
                throw new IllegalStateException("No distributor route configured for task type: " + profile.taskType());
            }
            log.warn("Using legacy distributor route fallback for taskId={} taskType={}", taskId, profile.taskType());
        }

        List<RouteNode> route = new ArrayList<>();
        for (Step step : configured) {
            if (!stageEnabled(profile, step)) {
                continue;
            }
            StageRegistry.StagePolicy policy = registry.require(step.stage());
            String subStage = normalizeSubStage(step.subStage());
            route.add(new RouteNode(
                    step.stage() + ":" + subStage,
                    step.stage(),
                    subStage,
                    label(step.label()),
                    route.size() + 1,
                    policy.tableName()
            ));
        }
        if (route.isEmpty()) {
            throw new IllegalStateException("Distributor route is empty for task: " + taskId);
        }
        return List.copyOf(route);
    }

    private static String label(String configuredLabel) {
        if (configuredLabel == null || configuredLabel.isBlank()) {
            return "Unknown";
        }
        return configuredLabel;
    }

    private static boolean stageEnabled(TaskProfile profile, Step step) {
        if (!"narration".equals(profile.taskType())) {
            return profile.hasBackgroundAudio() || !"demucs".equals(step.stage());
        }
        if ("demucs".equals(step.stage())) {
            return Boolean.FALSE.equals(profile.hasNativeSubtitle())
                    && profile.hasBackgroundAudio();
        }
        if ("downloader".equals(step.stage()) && "audio".equals(step.subStage())) {
            return Boolean.FALSE.equals(profile.hasNativeSubtitle());
        }
        if ("whisper".equals(step.stage()) && "source_transcription".equals(step.subStage())) {
            return Boolean.FALSE.equals(profile.hasNativeSubtitle());
        }
        if ("publisher".equals(step.stage()) && "script_generation".equals(step.subStage())) {
            return profile.hasNativeSubtitle() != null;
        }
        return true;
    }

    private static String normalizeSubStage(String value) {
        return value == null || value.isBlank() ? "main" : value.trim();
    }

    private static List<Step> steps(String... stages) {
        List<Step> result = new ArrayList<>();
        for (String stage : stages) result.add(new Step(stage, "main", null));
        return List.copyOf(result);
    }

    private record TaskProfile(
            String taskType,
            boolean hasBackgroundAudio,
            Boolean hasNativeSubtitle
    ) {
    }

    private record Step(String stage, String subStage, String label) {
    }
}
