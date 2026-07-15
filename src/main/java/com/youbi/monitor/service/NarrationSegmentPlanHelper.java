package com.youbi.monitor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class NarrationSegmentPlanHelper {
    static final int MAX_SEGMENT_CHARS = 500;

    private final ObjectMapper objectMapper;

    NarrationSegmentPlanHelper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    List<Integer> parseEndLineIds(String rawResponse) throws IOException {
        String value = text(rawResponse);
        if (value.isBlank()) {
            throw new IOException("大模型返回内容不能为空");
        }
        if (value.startsWith("```")) {
            value = value.replaceFirst("^```(?:json)?\\s*", "").replaceFirst("\\s*```$", "");
        }
        JsonNode root;
        try {
            root = objectMapper.readTree(value);
        } catch (JsonProcessingException firstError) {
            int start = value.indexOf('{');
            int end = value.lastIndexOf('}');
            if (start < 0 || end <= start) {
                throw new IOException("大模型返回值不是合法 JSON");
            }
            root = objectMapper.readTree(value.substring(start, end + 1));
        }
        JsonNode idsNode = root.path("end_line_ids");
        if (!idsNode.isArray() || idsNode.isEmpty()) {
            throw new IOException("返回 JSON 必须包含非空 end_line_ids 数组");
        }
        List<Integer> ids = new ArrayList<>();
        for (JsonNode node : idsNode) {
            if (!node.isIntegralNumber()) {
                throw new IOException("end_line_ids 只能包含整数");
            }
            ids.add(node.intValue());
        }
        return ids;
    }

    void validateEndLineIds(List<String> lines, List<Integer> ids) throws IOException {
        int previous = 0;
        for (int end : ids) {
            if (end <= previous || end > lines.size()) {
                throw new IOException("end_line_ids 必须严格递增且不能超过总行数 " + lines.size());
            }
            int chars = segmentChars(lines, previous, end);
            if (chars > MAX_SEGMENT_CHARS) {
                throw new IOException("第 " + (previous + 1) + "-" + end + " 行合并后为 "
                        + chars + " 字，超过 " + MAX_SEGMENT_CHARS);
            }
            previous = end;
        }
        if (ids.get(ids.size() - 1) != lines.size()) {
            throw new IOException("end_line_ids 最后一个值必须是末行 ID " + lines.size());
        }
    }

    List<Integer> compactEndLineIds(List<String> lines, List<Integer> ids) throws IOException {
        List<Integer> compacted = new ArrayList<>();
        int compactedStart = 0;
        int previousEnd = 0;
        for (int end : ids) {
            if (segmentChars(lines, compactedStart, end) > MAX_SEGMENT_CHARS) {
                if (previousEnd <= compactedStart) {
                    throw new IOException("无法生成不超过 " + MAX_SEGMENT_CHARS + " 字的分段");
                }
                compacted.add(previousEnd);
                compactedStart = previousEnd;
            }
            previousEnd = end;
        }
        if (compacted.isEmpty() || compacted.get(compacted.size() - 1) != lines.size()) {
            compacted.add(lines.size());
        }
        return compacted;
    }

    List<Integer> assignments(int lineCount, List<Integer> endLineIds) {
        List<Integer> result = new ArrayList<>(lineCount);
        int start = 0;
        for (int segment = 1; segment <= endLineIds.size(); segment++) {
            int end = endLineIds.get(segment - 1);
            while (start < end) {
                result.add(segment);
                start++;
            }
        }
        return result;
    }

    private int segmentChars(List<String> lines, int startInclusive, int endExclusive) {
        int chars = Math.max(0, endExclusive - startInclusive - 1);
        for (int index = startInclusive; index < endExclusive; index++) {
            chars += lines.get(index).length();
        }
        return chars;
    }

    private static String text(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }
}
