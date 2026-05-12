package com.youbi.monitor;

record StageDefinition(String key, String label, String statusColumn, String startedAtColumn,
                       String completedAtColumn, String errorColumn) {
}
