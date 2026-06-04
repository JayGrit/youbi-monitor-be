package com.youbi.monitor.repository;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface RowCallbackHandler {
    void processRow(ResultSet rs) throws SQLException;
}
