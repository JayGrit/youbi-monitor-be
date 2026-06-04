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
        String normalized = text(author);
        if (normalized.isBlank()) {
            return new MonitorService.SubmitterAuthorType("", "", true, true, true, "英文", "中文");
        }
        List<Map<String, Object>> rows = repository.queryForList("""
                SELECT type, need_subtitle, need_dubbing, need_separation, source_language, target_language
                FROM submitter_author
                WHERE author = ?
                LIMIT 1
                """, normalized);
        if (rows.isEmpty()) {
            return new MonitorService.SubmitterAuthorType(normalized, "", true, true, true, "英文", "中文");
        }
        Map<String, Object> row = rows.get(0);
        boolean needSubtitle = boolValue(row.get("need_subtitle"), true);
        return new MonitorService.SubmitterAuthorType(
                normalized,
                stringValue(row.get("type")),
                needSubtitle,
                needSubtitle && boolValue(row.get("need_dubbing"), true),
                boolValue(row.get("need_separation"), true),
                defaultLanguage(row.get("source_language"), "英文"),
                defaultLanguage(row.get("target_language"), "中文")
        );
    }

    
    public List<MonitorService.SubmitterAuthorType> listSubmitterAuthorTypes() {
        return repository.query("""
                SELECT author, type, need_subtitle, need_dubbing, need_separation, source_language, target_language
                FROM submitter_author
                ORDER BY CASE WHEN COALESCE(NULLIF(type, ''), '') = '' THEN 1 ELSE 0 END, type, author
                """, (rs, rowNum) -> new MonitorService.SubmitterAuthorType(
                text(rs.getString("author")),
                text(rs.getString("type")),
                boolValue(rs.getObject("need_subtitle"), true),
                boolValue(rs.getObject("need_subtitle"), true) && boolValue(rs.getObject("need_dubbing"), true),
                boolValue(rs.getObject("need_separation"), true),
                defaultLanguage(rs.getString("source_language"), "英文"),
                defaultLanguage(rs.getString("target_language"), "中文")
        ));
    }

    
    public MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            String author,
            String type,
            Boolean needSubtitle,
            Boolean needDubbing,
            Boolean needSeparation,
            String sourceLanguage,
            String targetLanguage
    ) {
        String normalizedAuthor = text(author);
        String normalizedType = text(type);
        String normalizedSourceLanguage = defaultLanguage(sourceLanguage, "英文");
        String normalizedTargetLanguage = defaultLanguage(targetLanguage, "中文");
        if (normalizedAuthor.isBlank()) {
            throw new IllegalArgumentException("author is required");
        }
        if (normalizedType.isBlank()) {
            throw new IllegalArgumentException("type is required");
        }
        boolean normalizedNeedSubtitle = !Boolean.FALSE.equals(needSubtitle);
        boolean normalizedNeedDubbing = normalizedNeedSubtitle && !Boolean.FALSE.equals(needDubbing);
        boolean normalizedNeedSeparation = !Boolean.FALSE.equals(needSeparation);
        repository.update("""
                INSERT INTO submitter_author (author, type, need_subtitle, need_dubbing, need_separation, source_language, target_language)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    type = VALUES(type),
                    need_subtitle = VALUES(need_subtitle),
                    need_dubbing = VALUES(need_dubbing),
                    need_separation = VALUES(need_separation),
                    source_language = VALUES(source_language),
                    target_language = VALUES(target_language),
                    updated_at = NOW()
                """,
                normalizedAuthor,
                normalizedType,
                normalizedNeedSubtitle ? 1 : 0,
                normalizedNeedDubbing ? 1 : 0,
                normalizedNeedSeparation ? 1 : 0,
                normalizedSourceLanguage,
                normalizedTargetLanguage
        );
        return new MonitorService.SubmitterAuthorType(
                normalizedAuthor,
                normalizedType,
                normalizedNeedSubtitle,
                normalizedNeedDubbing,
                normalizedNeedSeparation,
                normalizedSourceLanguage,
                normalizedTargetLanguage
        );
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
