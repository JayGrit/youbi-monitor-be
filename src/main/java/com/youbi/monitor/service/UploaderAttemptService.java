package com.youbi.monitor.service;

import com.youbi.monitor.repository.MonitorRepository;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
public class UploaderAttemptService {
    private static final String ATTEMPT_COLUMN = "upload_attempt_no";
    private static final Map<String, String> PLATFORM_TABLES = Map.of(
            "bilibili", "uploader_task_bilibili",
            "douyin", "uploader_task_douyin",
            "xiaohongshu", "uploader_task_xiaohongshu",
            "shipinhao", "uploader_task_shipinhao",
            "kuaishou", "uploader_task_kuaishou",
            "jinritoutiao", "uploader_task_jinritoutiao"
    );

    private final MonitorRepository repository;

    public UploaderAttemptService(MonitorRepository repository) {
        this.repository = repository;
    }

    @PostConstruct
    void ensureSchema() {
        PLATFORM_TABLES.values().forEach(table -> {
            if (tableExists(table) && !columnExists(table, ATTEMPT_COLUMN)) {
                repository.execute("ALTER TABLE " + table + " ADD COLUMN " + ATTEMPT_COLUMN + " INT NOT NULL DEFAULT 0");
            }
        });
    }

    @Transactional
    public String nextRunId(String taskId, String platform, String accountKey) {
        String normalizedTaskId = TextSupport.firstText(taskId, "manual");
        String normalizedPlatform = TextSupport.text(platform);
        String normalizedAccountKey = TextSupport.text(accountKey);
        String table = PLATFORM_TABLES.get(normalizedPlatform);
        if (table == null || !tableExists(table)) {
            return fallbackRunId(normalizedTaskId, normalizedPlatform, normalizedAccountKey);
        }
        ensureTableColumn(table);
        int updated = 0;
        if (!normalizedAccountKey.isBlank()) {
            updated = repository.update(
                    "UPDATE " + table + " SET " + ATTEMPT_COLUMN + " = LAST_INSERT_ID(" + ATTEMPT_COLUMN + " + 1) WHERE task_id = ? AND account_key = ?",
                    normalizedTaskId,
                    normalizedAccountKey
            );
        }
        if (updated <= 0) {
            updated = incrementByTaskId(table, normalizedTaskId);
        }
        if (updated <= 0) {
            return fallbackRunId(normalizedTaskId, normalizedPlatform, normalizedAccountKey);
        }
        Integer attemptNo = repository.queryForObject("SELECT LAST_INSERT_ID()", Integer.class);
        return buildRunId(normalizedTaskId, normalizedPlatform, normalizedAccountKey, attemptNo == null || attemptNo < 1 ? 1 : attemptNo);
    }

    private int incrementByTaskId(String table, String taskId) {
        int updated = repository.update(
                "UPDATE " + table + " SET " + ATTEMPT_COLUMN + " = LAST_INSERT_ID(" + ATTEMPT_COLUMN + " + 1) WHERE task_id = ?",
                taskId
        );
        return updated;
    }

    private void ensureTableColumn(String table) {
        if (tableExists(table) && !columnExists(table, ATTEMPT_COLUMN)) {
            repository.execute("ALTER TABLE " + table + " ADD COLUMN " + ATTEMPT_COLUMN + " INT NOT NULL DEFAULT 0");
        }
    }

    private boolean tableExists(String table) {
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = ?
                """, Integer.class, table);
        return count != null && count > 0;
    }

    private boolean columnExists(String table, String column) {
        Integer count = repository.queryForObject("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
                """, Integer.class, table, column);
        return count != null && count > 0;
    }

    private String buildRunId(String taskId, String platform, String accountKey, int attemptNo) {
        String accountPart = TextSupport.text(accountKey).isBlank() ? "default" : TextSupport.safeSegment(accountKey);
        String platformPart = TextSupport.text(platform).isBlank() ? "upload" : TextSupport.safeSegment(platform);
        return TextSupport.safeSegment(taskId) + "-" + platformPart + "-" + accountPart + "-" + attemptNo;
    }

    private String fallbackRunId(String taskId, String platform, String accountKey) {
        return buildRunId(taskId, platform, accountKey, 1) + "-" + System.currentTimeMillis();
    }
}
