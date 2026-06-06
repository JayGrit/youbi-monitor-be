package com.youbi.monitor.dto;

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
    private static final int SHORT_TITLE_MIN_LENGTH = 6;
    private static final int SHORT_TITLE_MAX_LENGTH = 16;
    private static final String SHORT_TITLE_FALLBACK = "精彩视频";
    private static final String SHORT_TITLE_PADDING = "视频";
    private static final String ALLOWED_SHORT_TITLE_SYMBOLS = "《》〈〉“”‘’\"'：:+＋？?％%℃°";

    public ShipinhaoUploadRequest {
        title = stripChineseDubbedMarker(title);
        shortTitle = normalizeShortTitle(shortTitle, title);
    }

    private static String stripChineseDubbedMarker(String value) {
        if (value == null) {
            return null;
        }
        return value.replace(CHINESE_DUBBED_MARKER, "").trim();
    }

    private static String normalizeShortTitle(String shortTitle, String title) {
        String source = stripChineseDubbedMarker(shortTitle);
        if (source == null || source.isBlank()) {
            source = title;
        }

        StringBuilder builder = new StringBuilder();
        boolean previousWasSpace = false;
        for (int offset = 0; offset < text(source).length();) {
            int codePoint = text(source).codePointAt(offset);
            offset += Character.charCount(codePoint);

            boolean comma = codePoint == ',' || codePoint == '，';
            boolean space = Character.isWhitespace(codePoint) || comma;
            if (space) {
                if (!builder.isEmpty() && !previousWasSpace) {
                    builder.append(' ');
                    previousWasSpace = true;
                }
                continue;
            }
            if (Character.isLetterOrDigit(codePoint)
                    || ALLOWED_SHORT_TITLE_SYMBOLS.indexOf(codePoint) >= 0) {
                builder.appendCodePoint(codePoint);
                previousWasSpace = false;
            }
        }

        String value = builder.toString().trim();
        if (value.isBlank()) {
            value = SHORT_TITLE_FALLBACK;
        }
        value = truncate(value, SHORT_TITLE_MAX_LENGTH);
        while (value.codePointCount(0, value.length()) < SHORT_TITLE_MIN_LENGTH) {
            value += SHORT_TITLE_PADDING;
        }
        return truncate(value, SHORT_TITLE_MAX_LENGTH);
    }

    private static String truncate(String value, int maxLength) {
        int codePointCount = value.codePointCount(0, value.length());
        if (codePointCount <= maxLength) {
            return value;
        }
        return value.substring(0, value.offsetByCodePoints(0, maxLength));
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }
}
