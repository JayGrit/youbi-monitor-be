package com.youbi.monitor;

public record ShipinhaoUploadRequest(
        String accountKey,
        String taskId,
        String videoPath,
        String videoUrl,
        String minioUrl,
        String videoLocation,
        String alidriveFileId,
        String alidriveRemotePath,
        String title,
        String description,
        String tags,
        String coverPath,
        String coverUrl,
        String shortTitle,
        Boolean draft
) {
    private static final String CHINESE_DUBBED_MARKER = "【中配】";

    public ShipinhaoUploadRequest {
        title = stripChineseDubbedMarker(title);
        shortTitle = stripChineseDubbedMarker(shortTitle);
    }

    private static String stripChineseDubbedMarker(String value) {
        if (value == null) {
            return null;
        }
        return value.replace(CHINESE_DUBBED_MARKER, "").trim();
    }
}
