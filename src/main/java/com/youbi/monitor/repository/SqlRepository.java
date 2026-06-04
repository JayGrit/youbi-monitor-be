package com.youbi.monitor.repository;

import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class SqlRepository {
    private final SqlSessionTemplate sqlSessionTemplate;

    public SqlRepository(SqlSessionTemplate sqlSessionTemplate) {
        this.sqlSessionTemplate = sqlSessionTemplate;
    }

    public void execute(String sql) {
        withStatement(sql, statement -> {
            statement.execute(sql);
            return null;
        });
    }

    public int update(String sql, Object... args) {
        return withPreparedStatement(sql, args, statement -> statement.executeUpdate());
    }

    public Long insertAndReturnKey(String sql, Object... args) {
        return withPreparedStatement(sql, args, Statement.RETURN_GENERATED_KEYS, statement -> {
            statement.executeUpdate();
            try (ResultSet keys = statement.getGeneratedKeys()) {
                if (!keys.next()) {
                    return null;
                }
                Number key = (Number) keys.getObject(1);
                return key == null ? null : key.longValue();
            }
        });
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
        return withPreparedStatement(sql, args, statement -> {
            try (ResultSet rs = statement.executeQuery()) {
                List<T> rows = new ArrayList<>();
                int rowNum = 0;
                while (rs.next()) {
                    rows.add(rowMapper.mapRow(rs, rowNum++));
                }
                return rows;
            }
        });
    }

    public void query(String sql, RowCallbackHandler callback, Object... args) {
        withPreparedStatement(sql, args, statement -> {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    callback.processRow(rs);
                }
                return null;
            }
        });
    }

    public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
        List<T> rows = queryForList(sql, requiredType, args);
        return singleResult(rows);
    }

    public <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... args) {
        List<T> rows = query(sql, rowMapper, args);
        return singleResult(rows);
    }

    private <T> T singleResult(List<T> rows) {
        if (rows.isEmpty()) {
            throw new EmptyResultDataAccessException(1);
        }
        if (rows.size() > 1) {
            throw new IllegalStateException("Expected one row but found " + rows.size());
        }
        return rows.get(0);
    }

    public <T> List<T> queryForList(String sql, Class<T> elementType, Object... args) {
        return query(sql, (rs, rowNum) -> convert(rs.getObject(1), elementType), args);
    }

    public List<Map<String, Object>> queryForList(String sql, Object... args) {
        return query(sql, (rs, rowNum) -> mapRow(rs), args);
    }

    private Map<String, Object> mapRow(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String label = metaData.getColumnLabel(i);
            if (label == null || label.isBlank()) {
                label = metaData.getColumnName(i);
            }
            row.put(label, rs.getObject(i));
        }
        return row;
    }

    @SuppressWarnings("unchecked")
    private <T> T convert(Object value, Class<T> requiredType) {
        if (value == null) {
            return null;
        }
        if (requiredType.isInstance(value)) {
            return requiredType.cast(value);
        }
        if (requiredType == String.class) {
            return (T) String.valueOf(value);
        }
        if (requiredType == Integer.class || requiredType == int.class) {
            return (T) Integer.valueOf(((Number) value).intValue());
        }
        if (requiredType == Long.class || requiredType == long.class) {
            return (T) Long.valueOf(((Number) value).longValue());
        }
        if (requiredType == Boolean.class || requiredType == boolean.class) {
            if (value instanceof Number number) {
                return (T) Boolean.valueOf(number.intValue() != 0);
            }
            return (T) Boolean.valueOf(String.valueOf(value));
        }
        if (requiredType == LocalDateTime.class) {
            if (value instanceof Timestamp timestamp) {
                return (T) timestamp.toLocalDateTime();
            }
            return (T) value;
        }
        return (T) value;
    }

    private void bind(PreparedStatement statement, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            statement.setObject(i + 1, args[i]);
        }
    }

    private <T> T withStatement(String sql, SqlStatementCallback<T> callback) {
        SqlSession sqlSession = sqlSessionTemplate;
        Connection connection = sqlSession.getConnection();
        try (Statement statement = connection.createStatement()) {
            return callback.doInStatement(statement);
        } catch (SQLException exception) {
            throw new IllegalStateException("Database statement failed", exception);
        }
    }

    private <T> T withPreparedStatement(String sql, Object[] args, PreparedStatementCallback<T> callback) {
        return withPreparedStatement(sql, args, Statement.NO_GENERATED_KEYS, callback);
    }

    private <T> T withPreparedStatement(String sql, Object[] args, int autoGeneratedKeys, PreparedStatementCallback<T> callback) {
        SqlSession sqlSession = sqlSessionTemplate;
        Connection connection = sqlSession.getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sql, autoGeneratedKeys)) {
            bind(statement, args);
            return callback.doInPreparedStatement(statement);
        } catch (SQLException exception) {
            throw new IllegalStateException("Database statement failed", exception);
        }
    }

    @FunctionalInterface
    private interface SqlStatementCallback<T> {
        T doInStatement(Statement statement) throws SQLException;
    }

    @FunctionalInterface
    private interface PreparedStatementCallback<T> {
        T doInPreparedStatement(PreparedStatement statement) throws SQLException;
    }
}
