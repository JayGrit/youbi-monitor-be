package com.youbi.monitor;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan("com.youbi.monitor.repository")
@SpringBootApplication
public class MonitorBeApplication {
    public static void main(String[] args) {
        SpringApplication.run(MonitorBeApplication.class, args);
    }
}
