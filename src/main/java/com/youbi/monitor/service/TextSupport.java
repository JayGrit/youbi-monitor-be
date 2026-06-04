package com.youbi.monitor.service;

import java.io.IOException;

final class TextSupport {
    private TextSupport() {
    }

    static String text(String value) {
        return value == null ? "" : value.trim();
    }

    static boolean hasText(String value) {
        return !text(value).isBlank();
    }

    static String required(String value, String field) throws IOException {
        String text = text(value);
        if (text.isBlank()) {
            throw new IOException("Missing field: " + field);
        }
        return text;
    }

    static String firstText(String... values) {
        for (String value : values) {
            String text = text(value);
            if (!text.isBlank()) {
                return text;
            }
        }
        return "";
    }

    static boolean containsAny(String value, String... needles) {
        String text = text(value);
        for (String needle : needles) {
            if (text.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    static String truncate(String value, int max) {
        String text = text(value);
        if (text.codePointCount(0, text.length()) <= max) {
            return text;
        }
        return text.codePoints()
                .limit(max)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    static String truncateWithEllipsis(String value, int max) {
        String text = text(value);
        if (text.codePointCount(0, text.length()) <= max) {
            return text;
        }
        return text.codePoints()
                .limit(max - 3L)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .append("...")
                .toString();
    }

    static String safeSegment(String value) {
        String sanitized = text(value).replaceAll("[^A-Za-z0-9._-]+", "_");
        return sanitized.isBlank() ? "manual" : sanitized;
    }
}
