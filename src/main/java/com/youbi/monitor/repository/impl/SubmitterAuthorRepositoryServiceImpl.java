package com.youbi.monitor.repository.impl;

import com.youbi.monitor.repository.ISubmitterAuthorRepositoryService;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.service.MonitorService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SubmitterAuthorRepositoryServiceImpl extends MonitorRepositorySqlSupport implements ISubmitterAuthorRepositoryService {
    public SubmitterAuthorRepositoryServiceImpl(MonitorRepository repository) {
        super(repository);
    }

    
    public MonitorService.SubmitterAuthorType findSubmitterAuthorType(String author) {
        ensureAuthorCoverColumns();
        String normalized = text(author);
        if (normalized.isBlank()) {
            return new MonitorService.SubmitterAuthorType("", "", "", true, "英文", "中文", false, "", false, 0, 0);
        }
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT type, task_type, has_background_audio, source_language, target_language, reset_cover, cover_orientation, fetch_new_videos
                FROM submitter_author
                WHERE author = ?
                LIMIT 1
                """, normalized);
        if (rows.isEmpty()) {
            return new MonitorService.SubmitterAuthorType(normalized, "", "", true, "英文", "中文", false, "", false, 0, 0);
        }
        Map<String, Object> row = rows.get(0);
        boolean resetCover = boolValue(row.get("reset_cover"), false);
        return new MonitorService.SubmitterAuthorType(
                normalized,
                stringValue(row.get("type")),
                stringValue(row.get("task_type")),
                boolValue(row.get("has_background_audio"), true),
                defaultLanguage(row.get("source_language"), "英文"),
                defaultLanguage(row.get("target_language"), "中文"),
                resetCover,
                resetCover ? normalizeCoverOrientation(row.get("cover_orientation")) : "",
                boolValue(row.get("fetch_new_videos"), false),
                0,
                0
        );
    }

    
    public List<MonitorService.SubmitterAuthorType> listSubmitterAuthorTypes() {
        ensureAuthorCoverColumns();
        return repository.query("""
                SELECT author, type, task_type, has_background_audio, source_language, target_language, reset_cover, cover_orientation, fetch_new_videos
                FROM submitter_author
                ORDER BY CASE WHEN COALESCE(NULLIF(type, ''), '') = '' THEN 1 ELSE 0 END, type, author
                """, (rs, rowNum) -> {
            boolean resetCover = boolValue(rs.getObject("reset_cover"), false);
            return new MonitorService.SubmitterAuthorType(
                    text(rs.getString("author")),
                    text(rs.getString("type")),
                    text(rs.getString("task_type")),
                    boolValue(rs.getObject("has_background_audio"), true),
                    defaultLanguage(rs.getString("source_language"), "英文"),
                    defaultLanguage(rs.getString("target_language"), "中文"),
                    resetCover,
                    resetCover ? normalizeCoverOrientation(rs.getString("cover_orientation")) : "",
                    boolValue(rs.getObject("fetch_new_videos"), false),
                    0,
                    0
            );
        });
    }

    
    public MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            String author,
            String type,
            String taskType,
            Boolean hasBackgroundAudio,
            String sourceLanguage,
            String targetLanguage,
            Boolean resetCover,
            String coverOrientation,
            Boolean fetchNewVideos
    ) {
        ensureAuthorCoverColumns();
        String normalizedAuthor = text(author);
        String normalizedType = text(type);
        String normalizedTaskType = text(taskType);
        String normalizedSourceLanguage = defaultLanguage(sourceLanguage, "英文");
        String normalizedTargetLanguage = defaultLanguage(targetLanguage, "中文");
        if (normalizedAuthor.isBlank()) {
            throw new IllegalArgumentException("author is required");
        }
        if (normalizedType.isBlank()) {
            throw new IllegalArgumentException("type is required");
        }
        if (normalizedTaskType.isBlank()) {
            throw new IllegalArgumentException("taskType is required");
        }
        if (repository.queryForList(
                "SELECT task_type FROM distributor_task_type WHERE task_type = ? AND enabled = 1",
                normalizedTaskType
        ).isEmpty()) {
            throw new IllegalArgumentException("taskType is invalid");
        }
        boolean normalizedHasBackgroundAudio = !Boolean.FALSE.equals(hasBackgroundAudio);
        boolean normalizedResetCover = Boolean.TRUE.equals(resetCover);
        String normalizedCoverOrientation = normalizedResetCover ? normalizeCoverOrientation(coverOrientation) : "";
        boolean normalizedFetchNewVideos = Boolean.TRUE.equals(fetchNewVideos);
        repository.update("""
                INSERT INTO submitter_author (author, type, task_type, has_background_audio, source_language, target_language, reset_cover, cover_orientation, fetch_new_videos)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    type = VALUES(type),
                    task_type = VALUES(task_type),
                    has_background_audio = VALUES(has_background_audio),
                    source_language = VALUES(source_language),
                    target_language = VALUES(target_language),
                    reset_cover = VALUES(reset_cover),
                    cover_orientation = VALUES(cover_orientation),
                    fetch_new_videos = VALUES(fetch_new_videos),
                    updated_at = NOW()
                """,
                normalizedAuthor,
                normalizedType,
                normalizedTaskType,
                normalizedHasBackgroundAudio ? 1 : 0,
                normalizedSourceLanguage,
                normalizedTargetLanguage,
                normalizedResetCover ? 1 : 0,
                normalizedCoverOrientation,
                normalizedFetchNewVideos ? 1 : 0
        );
        return new MonitorService.SubmitterAuthorType(
                normalizedAuthor,
                normalizedType,
                normalizedTaskType,
                normalizedHasBackgroundAudio,
                normalizedSourceLanguage,
                normalizedTargetLanguage,
                normalizedResetCover,
                normalizedCoverOrientation,
                normalizedFetchNewVideos,
                0,
                0
        );
    }

    private void ensureAuthorCoverColumns() {
        if (!columnExists("submitter_author", "reset_cover")) {
            repository.update("""
                    ALTER TABLE submitter_author
                    ADD COLUMN reset_cover TINYINT(1) NOT NULL DEFAULT 0
                    """);
        }
        if (!columnExists("submitter_author", "cover_orientation")) {
            repository.update("""
                    ALTER TABLE submitter_author
                    ADD COLUMN cover_orientation VARCHAR(16) NOT NULL DEFAULT ''
                    """);
        }
        if (!columnExists("submitter_author", "fetch_new_videos")) {
            repository.update("""
                    ALTER TABLE submitter_author
                    ADD COLUMN fetch_new_videos TINYINT(1) NOT NULL DEFAULT 0
                    """);
        }
    }

    private String normalizeCoverOrientation(Object value) {
        String normalized = stringValue(value).toLowerCase();
        if (normalized.equals("vertical")) {
            return "vertical";
        }
        if (normalized.equals("horizontal")) {
            return "horizontal";
        }
        return "horizontal";
    }

    
    public MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(String author) {
        String normalizedAuthor = text(author);
        if (normalizedAuthor.isBlank()) {
            throw new IllegalArgumentException("author is required");
        }
        int deletedAuthorRows = repository.update("DELETE FROM submitter_author WHERE author = ?", normalizedAuthor);
        int deletedVideoRows = 0;
        if (tableExists("submitter_video")) {
            deletedVideoRows = repository.update("""
                    DELETE FROM submitter_video
                    WHERE uploader = ? OR import_author = ? OR channel_id = ?
                    """, normalizedAuthor, normalizedAuthor, normalizedAuthor);
        }
        return new MonitorService.SubmitterAuthorDeleteResult("deleted", normalizedAuthor, deletedAuthorRows, deletedVideoRows);
    }


}
