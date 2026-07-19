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
                    new Step("downloader", "metadata"), new Step("downloader", "audio"),
                    new Step("demucs", "main"), new Step("whisper", "source_transcription"),
                    new Step("publisher", "script_generation"), new Step("publisher", "publish_metadata"),
                    new Step("publisher", "segment_plan"), new Step("publisher", "image_generation"),
                    new Step("asseter", "image_composition"), new Step("speaker", "narration"),
                    new Step("combiner", "audio_merge"), new Step("whisper", "main"),
                    new Step("asseter", "audio_visualization"), new Step("combiner", "video_render"),
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
                SELECT t.task_type, opts.has_background_audio, ts.narration_input_mode, opts.has_native_subtitle
                FROM task t
                LEFT JOIN task_source ts ON ts.task_id = t.id
                LEFT JOIN task_options opts ON opts.task_id = t.id
                WHERE t.id = ?
                """, (rs, rowNum) -> new TaskProfile(
                rs.getString("task_type"),
                rs.getObject("has_background_audio") == null || rs.getBoolean("has_background_audio"),
                rs.getString("narration_input_mode"),
                rs.getObject("has_native_subtitle") == null ? null : rs.getBoolean("has_native_subtitle")
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
            if (!stageEnabled(profile, step)) {
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
        if ("downloader".equals(stage) && "metadata".equals(subStage)) return "元数据下载";
        if ("downloader".equals(stage) && "video".equals(subStage)) return "视频下载";
        if ("downloader".equals(stage) && "audio".equals(subStage)) return "音频下载";
        if ("whisper".equals(stage) && "source_transcription".equals(subStage)) return "源语音识别";
        if ("publisher".equals(stage)) {
            return switch (subStage) {
                case "script_generation" -> "文案生成";
                case "publish_metadata" -> "发布准备";
                case "segment_plan" -> "文案分段";
                case "image_generation" -> "图片生成";
                default -> defaultLabel;
            };
        }
        if ("asseter".equals(stage)) {
            return switch (subStage) {
                case "image_composition" -> "图片素材";
                case "audio_visualization" -> "音频素材";
                default -> defaultLabel;
            };
        }
        if (!"combiner".equals(stage)) return defaultLabel;
        return switch (subStage) {
            case "audio_merge" -> "音频合并";
            case "video_render" -> "视频渲染";
            case "asmr" -> "ASMR 合成";
            default -> defaultLabel;
        };
    }

    private static boolean stageEnabled(TaskProfile profile, Step step) {
        if (!"narration".equals(profile.taskType())) {
            return profile.hasBackgroundAudio() || !"demucs".equals(step.stage());
        }
        if ("demucs".equals(step.stage())) {
            return "submission".equals(normalizeSubStage(profile.narrationInputMode()))
                    && Boolean.FALSE.equals(profile.hasNativeSubtitle())
                    && profile.hasBackgroundAudio();
        }
        if ("prepared_text".equals(normalizeSubStage(profile.narrationInputMode()))) {
            return !(("downloader".equals(step.stage()) && "metadata".equals(step.subStage()))
                    || ("downloader".equals(step.stage()) && "audio".equals(step.subStage()))
                    || ("whisper".equals(step.stage()) && "source_transcription".equals(step.subStage()))
                    || ("publisher".equals(step.stage()) && "script_generation".equals(step.subStage())));
        }
        if (!"submission".equals(normalizeSubStage(profile.narrationInputMode()))) {
            return true;
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
        for (String stage : stages) result.add(new Step(stage, "main"));
        return List.copyOf(result);
    }

    private record TaskProfile(
            String taskType,
            boolean hasBackgroundAudio,
            String narrationInputMode,
            Boolean hasNativeSubtitle
    ) {
    }

    private record Step(String stage, String subStage) {
    }
}
