package com.youbi.monitor.repository;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MonitorAsyncUploadRepository extends SqlRepository {
    public MonitorAsyncUploadRepository(SqlSessionTemplate sqlSessionTemplate) {
        super(sqlSessionTemplate);
    }
}
