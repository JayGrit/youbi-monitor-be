package com.youbi.monitor.repository;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class QueueMonitorRepository extends SqlRepository {
    public QueueMonitorRepository(SqlSessionTemplate sqlSessionTemplate) {
        super(sqlSessionTemplate);
    }
}
