package com.youbi.monitor;

import java.util.List;

public record SocialRiskState(
        String code,
        String severity,
        String message,
        boolean blocking,
        List<String> matchedKeywords
) {
    static SocialRiskState normal() {
        return new SocialRiskState("normal", "info", "未识别到明确风控状态", false, List.of());
    }
}
