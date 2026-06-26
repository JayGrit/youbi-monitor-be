package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.ISpeakerSegmentRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.service.MonitorService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SpeakerSegmentRepositoryServiceImpl extends MonitorRepositorySqlSupport implements ISpeakerSegmentRepositoryService {
    public SpeakerSegmentRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    
    public MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(String taskId, long segmentId, String dstText) {
        String normalizedText = dstText == null ? "" : dstText;
        int updated = repository.update("""
                UPDATE speaker_segment
                SET dst_text = ?
                WHERE task_id = ? AND id = ?
                """, normalizedText, taskId, segmentId);
        if (updated == 0) {
            return null;
        }
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT *
                FROM speaker_segment
                WHERE task_id = ? AND id = ?
                LIMIT 1
                """, taskId, segmentId);
        Map<String, Object> row = rows.isEmpty() ? Map.of() : rows.get(0);
        return new MonitorService.SpeakerSegmentTextUpdateResult(
                segmentId,
                taskId,
                row.get("item_index") instanceof Number itemIndex ? itemIndex.intValue() : null,
                stringValue(row.get("dst_text")),
                localDateTime(row.get("updated_at"))
        );
    }

    public MonitorService.TranslatorSegmentTextUpdateResult updateTranslatorSegmentDstText(String taskId, int itemIndex, String dstText) {
        String normalizedText = dstText == null ? "" : dstText;
        int updated = repository.update("""
                UPDATE translator_segment
                SET dst_text = ?
                WHERE task_id = ? AND item_index = ?
                """, normalizedText, taskId, itemIndex);
        if (updated == 0) {
            return null;
        }
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT *
                FROM translator_segment
                WHERE task_id = ? AND item_index = ?
                LIMIT 1
                """, taskId, itemIndex);
        Map<String, Object> row = rows.isEmpty() ? Map.of() : rows.get(0);
        return new MonitorService.TranslatorSegmentTextUpdateResult(
                taskId,
                itemIndex,
                stringValue(row.get("dst_text")),
                localDateTime(row.get("updated_at"))
        );
    }

}
