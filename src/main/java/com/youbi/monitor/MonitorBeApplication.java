package com.youbi.monitor;

import org.apache.ibatis.annotations.Mapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@MapperScan(basePackages = "com.youbi.monitor.repository", annotationClass = Mapper.class)
@SpringBootApplication
@EnableScheduling
public class MonitorBeApplication {
    public static void main(String[] args) {
        SpringApplication.run(MonitorBeApplication.class, args);
    }
}
