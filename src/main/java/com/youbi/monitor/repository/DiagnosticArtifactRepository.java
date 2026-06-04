package com.youbi.monitor.repository;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DiagnosticArtifactRepository extends SqlRepository {
    public DiagnosticArtifactRepository(SqlSessionTemplate sqlSessionTemplate) {
        super(sqlSessionTemplate);
    }
}
