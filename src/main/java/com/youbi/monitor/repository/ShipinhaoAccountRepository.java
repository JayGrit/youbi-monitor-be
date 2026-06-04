package com.youbi.monitor.repository;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ShipinhaoAccountRepository extends SqlRepository {
    public ShipinhaoAccountRepository(SqlSessionTemplate sqlSessionTemplate) {
        super(sqlSessionTemplate);
    }
}
