package com.youbi.monitor.repository;

import java.util.List;

public final class RepositorySchemaSupport {
    private RepositorySchemaSupport() {
    }

    public static void ensureSurrogatePrimaryKey(SqlRepository repository, String table) {
        if (!tableExists(repository, table)) {
            return;
        }
        ensureIdColumn(repository, table);
        List<String> primaryColumns = repository.query(
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
        String accountKeyIndex = uniqueAccountKeyIndex(repository, table);
        if (primaryColumns.size() == 1 && "id".equals(primaryColumns.get(0))) {
            if (accountKeyIndex == null) {
                repository.execute("ALTER TABLE " + table + " ADD UNIQUE KEY " + uniqueAccountKeyName(table) + " (account_key)");
            }
            return;
        }
        String uniqueClause = accountKeyIndex == null
                ? ", ADD UNIQUE KEY " + uniqueAccountKeyName(table) + " (account_key)"
                : "";
        if (primaryColumns.isEmpty()) {
            repository.execute("ALTER TABLE " + table + " ADD PRIMARY KEY (id)" + uniqueClause);
            return;
        }
        repository.execute("ALTER TABLE " + table + " DROP PRIMARY KEY, ADD PRIMARY KEY (id)" + uniqueClause);
    }

    public static void ensureColumn(SqlRepository repository, String table, String column, String definition) {
        Integer count = repository.queryForObject(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                """,
                Integer.class,
                table,
                column
        );
        if (count == null || count == 0) {
            repository.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
    }

    private static boolean tableExists(SqlRepository repository, String table) {
        Integer count = repository.queryForObject(
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

    private static void ensureIdColumn(SqlRepository repository, String table) {
        Integer count = repository.queryForObject(
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
            repository.execute("ALTER TABLE " + table + " ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT UNIQUE FIRST");
        }
    }

    private static String uniqueAccountKeyIndex(SqlRepository repository, String table) {
        List<String> indexes = repository.query(
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
