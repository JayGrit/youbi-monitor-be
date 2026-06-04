package com.youbi.monitor.service;

import com.microsoft.playwright.Page;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SocialRiskDetector {
    SocialRiskState detect(SocialBrowserPlatform platform, Page page) {
        String url = "";
        try {
            url = page.url();
        } catch (Exception ignored) {
        }
        String body = PlaywrightDiagnostics.safeBodyText(page);
        String buttons = PlaywrightDiagnostics.visibleButtonTexts(page);
        return detect(platform, url, body + "\n" + buttons);
    }

    SocialRiskState detect(SocialBrowserPlatform platform, String url, String text) {
        String haystack = (safe(url) + "\n" + safe(text)).toLowerCase();
        SocialRiskState state = match("captcha_required", "error", "页面要求验证码/滑块/安全验证", true, haystack,
                "验证码", "安全验证", "滑块", "拖动滑块", "captcha", "verify", "验证身份", "环境异常");
        if (state != null) {
            return state;
        }
        state = match("login_required", "error", "登录态失效或需要重新登录", true, haystack,
                "扫码登录", "登录/注册", "请登录", "重新登录", "手机号登录", "账号登录", "login");
        if (state != null && !containsAny(haystack, "发布", "上传视频", "稿件", "创作服务")) {
            return state;
        }
        state = match("rate_limited", "error", "平台返回操作频繁/风控限制", true, haystack,
                "操作频繁", "投稿频繁", "请求频繁", "过于频繁", "风控", "限流", "稍后再试", "rate limit", "too frequent");
        if (state != null) {
            return state;
        }
        state = match("account_restricted", "error", "账号状态受限或需实名/资质处理", true, haystack,
                "实名", "资质", "账号异常", "账号状态异常", "账号受限", "处罚", "申诉", "封禁", "违规");
        if (state != null) {
            return state;
        }
        state = match("upload_processing", "warning", "素材仍在上传或处理中", false, haystack,
                "上传中", "处理中", "视频处理中", "转码中", "智能推荐封面生成中");
        if (state != null) {
            return state;
        }
        state = platformSpecific(platform, haystack);
        return state == null ? SocialRiskState.normal() : state;
    }

    private SocialRiskState platformSpecific(SocialBrowserPlatform platform, String haystack) {
        if (platform == SocialBrowserPlatform.DOUYIN) {
            return match("douyin_creator_check", "error", "抖音创作者中心要求额外校验", true, haystack,
                    "创作者登录", "抖音创作服务平台", "经营者认证");
        }
        if (platform == SocialBrowserPlatform.XIAOHONGSHU) {
            return match("xiaohongshu_creator_check", "error", "小红书创作者中心要求额外校验", true, haystack,
                    "小红书app扫一扫", "专业号", "创作服务平台异常");
        }
        if (platform == SocialBrowserPlatform.BILIBILI) {
            return match("bilibili_creator_check", "error", "Bilibili 创作中心要求额外校验", true, haystack,
                    "创作中心安全验证", "硬币不足", "请完成认证");
        }
        return null;
    }

    private SocialRiskState match(String code, String severity, String message, boolean blocking, String haystack, String... keywords) {
        List<String> matched = new ArrayList<>();
        for (String keyword : keywords) {
            if (haystack.contains(keyword.toLowerCase())) {
                matched.add(keyword);
            }
        }
        if (matched.isEmpty()) {
            return null;
        }
        return new SocialRiskState(code, severity, message, blocking, matched);
    }

    private boolean containsAny(String text, String... keywords) {
        for (String keyword : keywords) {
            if (text.contains(keyword.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }
}
