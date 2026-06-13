package com.youbi.monitor.service;

class PlaywrightUploadException extends RuntimeException {
    private final PlaywrightUploadErrorCode errorCode;

    PlaywrightUploadException(PlaywrightUploadErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    PlaywrightUploadException(PlaywrightUploadErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    PlaywrightUploadErrorCode errorCode() {
        return errorCode;
    }
}
