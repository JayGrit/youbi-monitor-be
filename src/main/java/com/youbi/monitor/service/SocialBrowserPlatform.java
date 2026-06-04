package com.youbi.monitor.service;

enum SocialBrowserPlatform {
    DOUYIN("douyin"),
    XIAOHONGSHU("xiaohongshu"),
    BILIBILI("bilibili"),
    SHIPINHAO("shipinhao"),
    KUAISHOU("kuaishou"),
    JINRITOUTIAO("jinritoutiao");

    private final String configKey;

    SocialBrowserPlatform(String configKey) {
        this.configKey = configKey;
    }

    String configKey() {
        return configKey;
    }
}
