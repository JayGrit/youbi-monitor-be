package com.youbi.monitor.service;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class StageRegistry {
    private static final Map<String, StagePolicy> POLICIES = createPolicies();

    public StagePolicy require(String stage) {
        StagePolicy policy = POLICIES.get(stage);
        if (policy == null) {
            throw new IllegalStateException("Distributor stage is not registered in monitor: " + stage);
        }
        return policy;
    }

    public List<StagePolicy> policies() {
        return List.copyOf(POLICIES.values());
    }

    private static Map<String, StagePolicy> createPolicies() {
        Map<String, StagePolicy> policies = new LinkedHashMap<>();
        register(policies, "downloader", "下载");
        register(policies, "publisher", "发布准备");
        register(policies, "demucs", "人声分离");
        register(policies, "whisper", "语音识别");
        register(policies, "translator", "翻译");
        register(policies, "speaker", "配音");
        register(policies, "asseter", "素材加工");
        register(policies, "combiner", "音视频合成");
        register(policies, "uploader", "上传");
        return Map.copyOf(policies);
    }

    private static void register(Map<String, StagePolicy> policies, String stage, String label) {
        policies.put(stage, new StagePolicy(stage, stage, label));
    }

    public record StagePolicy(String stage, String tableName, String label) {
    }
}
