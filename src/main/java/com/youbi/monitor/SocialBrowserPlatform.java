package com.youbi.monitor;

enum SocialBrowserPlatform {
    DOUYIN("douyin"),
    XIAOHONGSHU("xiaohongshu"),
    BILIBILI("bilibili");

    private final String configKey;

    SocialBrowserPlatform(String configKey) {
        this.configKey = configKey;
    }

    String configKey() {
        return configKey;
    }
}
