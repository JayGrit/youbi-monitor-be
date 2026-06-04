package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.WhisperProcessingDetail;
import com.youbi.monitor.model.WhisperWordTimestamp;
import com.youbi.monitor.repository.IWhisperProcessingRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class WhisperProcessingRepositoryServiceImpl extends MonitorRepositorySqlSupport implements IWhisperProcessingRepositoryService {
    public WhisperProcessingRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    @Override
    public List<WhisperWordTimestamp> listWhisperWordTimestamps(String taskId) {
        if (!tableExists("whisper_word_timestamp")) {
            return List.of();
        }
        return repository.query(
                """
                SELECT task_id, segment_index, word_index, text, start_time, end_time
                FROM whisper_word_timestamp
                WHERE task_id = ?
                ORDER BY segment_index, word_index, id
                """,
                (rs, rowNum) -> new WhisperWordTimestamp(
                        rs.getString("task_id"),
                        rs.getInt("segment_index"),
                        rs.getInt("word_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        );
    }

    @Override
    public WhisperProcessingDetail findWhisperProcessing(String taskId) {
        List<WhisperProcessingDetail.RawSegment> rawSegments = tableExists("whisper_raw_segment")
                ? repository.query(
                """
                SELECT id, raw_index, text, start_time, end_time
                FROM whisper_raw_segment
                WHERE task_id = ?
                ORDER BY raw_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.RawSegment(
                        rs.getLong("id"),
                        rs.getInt("raw_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        )
                : List.of();
        List<WhisperProcessingDetail.AlignedSegment> alignedSegments = tableExists("whisper_aligned_segment")
                ? repository.query(
                """
                SELECT id, raw_segment_id, aligned_index, text, start_time, end_time
                FROM whisper_aligned_segment
                WHERE task_id = ?
                ORDER BY aligned_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.AlignedSegment(
                        rs.getLong("id"),
                        nullableLong(rs, "raw_segment_id"),
                        rs.getInt("aligned_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        )
                : List.of();
        List<WhisperProcessingDetail.PysbdSegment> pysbdSegments = tableExists("whisper_pysbd_segment")
                ? repository.query(
                """
                SELECT id, pysbd_index, text, start_time, end_time
                FROM whisper_pysbd_segment
                WHERE task_id = ?
                ORDER BY pysbd_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.PysbdSegment(
                        rs.getLong("id"),
                        rs.getInt("pysbd_index"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time")
                ),
                taskId
        )
                : List.of();
        List<WhisperProcessingDetail.SplitSegment> splitSegments = tableExists("whisper_split")
                ? repository.query(
                """
                SELECT id, split_index, pysbd_segment_id, text, start_time, end_time,
                       split_reason, split_method, split_punctuation, split_conjunction
                FROM whisper_split
                WHERE task_id = ?
                ORDER BY split_index, id
                """,
                (rs, rowNum) -> new WhisperProcessingDetail.SplitSegment(
                        rs.getLong("id"),
                        rs.getInt("split_index"),
                        rs.getLong("pysbd_segment_id"),
                        rs.getString("text"),
                        rs.getInt("start_time"),
                        rs.getInt("end_time"),
                        rs.getString("split_reason"),
                        rs.getString("split_method"),
                        rs.getString("split_punctuation"),
                        rs.getString("split_conjunction")
                ),
                taskId
        )
                : List.of();
        return new WhisperProcessingDetail(rawSegments, alignedSegments, pysbdSegments, splitSegments);
    }


}
