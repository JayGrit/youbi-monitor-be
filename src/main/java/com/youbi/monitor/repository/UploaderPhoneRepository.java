package com.youbi.monitor.repository;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class UploaderPhoneRepository extends SqlRepository {
    public UploaderPhoneRepository(SqlSessionTemplate sqlSessionTemplate) {
        super(sqlSessionTemplate);
    }
}
