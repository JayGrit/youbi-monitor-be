package com.youbi.monitor.service;

enum PlaywrightUploadErrorCode {
    LOGIN_REQUIRED("LOGIN_REQUIRED"),
    PLAYWRIGHT_START_FAILED("PLAYWRIGHT_START_FAILED"),
    RATE_LIMITED("RATE_LIMITED"),
    UPLOAD_FAILED("UPLOAD_FAILED");

    private final String code;

    PlaywrightUploadErrorCode(String code) {
        this.code = code;
    }

    String code() {
        return code;
    }
}
