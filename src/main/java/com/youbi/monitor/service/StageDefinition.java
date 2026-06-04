package com.youbi.monitor.service;

record StageDefinition(String key, String label, String statusColumn, String startedAtColumn,
                       String completedAtColumn, String errorColumn) {
}
