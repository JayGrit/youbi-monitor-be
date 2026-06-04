package com.youbi.monitor.service;

import com.youbi.monitor.repository.DatabaseClient;

import java.util.List;

final class AccountTableSchemaSupport {
    private AccountTableSchemaSupport() {
    }

    static void ensureSurrogatePrimaryKey(DatabaseClient jdbcTemplate, String table) {
        if (!tableExists(jdbcTemplate, table)) {
            return;
        }
        ensureIdColumn(jdbcTemplate, table);
        List<String> primaryColumns = jdbcTemplate.query(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
                """,
                (rs, rowNum) -> rs.getString("COLUMN_NAME"),
                table
        );
        String accountKeyIndex = uniqueAccountKeyIndex(jdbcTemplate, table);
        if (primaryColumns.size() == 1 && "id".equals(primaryColumns.get(0))) {
            if (accountKeyIndex == null) {
                jdbcTemplate.execute("ALTER TABLE " + table + " ADD UNIQUE KEY " + uniqueAccountKeyName(table) + " (account_key)");
            }
            return;
        }
        String uniqueClause = accountKeyIndex == null
                ? ", ADD UNIQUE KEY " + uniqueAccountKeyName(table) + " (account_key)"
                : "";
        if (primaryColumns.isEmpty()) {
            jdbcTemplate.execute("ALTER TABLE " + table + " ADD PRIMARY KEY (id)" + uniqueClause);
            return;
        }
        jdbcTemplate.execute("ALTER TABLE " + table + " DROP PRIMARY KEY, ADD PRIMARY KEY (id)" + uniqueClause);
    }

    private static boolean tableExists(DatabaseClient jdbcTemplate, String table) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                """,
                Integer.class,
                table
        );
        return count != null && count > 0;
    }

    private static void ensureIdColumn(DatabaseClient jdbcTemplate, String table) {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = 'id'
                """,
                Integer.class,
                table
        );
        if (count == null || count == 0) {
            jdbcTemplate.execute("ALTER TABLE " + table + " ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT UNIQUE FIRST");
        }
    }

    private static String uniqueAccountKeyIndex(DatabaseClient jdbcTemplate, String table) {
        List<String> indexes = jdbcTemplate.query(
                """
                SELECT INDEX_NAME
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = 'account_key'
                  AND NON_UNIQUE = 0
                  AND INDEX_NAME <> 'PRIMARY'
                GROUP BY INDEX_NAME
                HAVING COUNT(*) = 1
                LIMIT 1
                """,
                (rs, rowNum) -> rs.getString("INDEX_NAME"),
                table
        );
        return indexes.stream().findFirst().orElse(null);
    }

    private static String uniqueAccountKeyName(String table) {
        return "uniq_" + table + "_account_key";
    }
}
