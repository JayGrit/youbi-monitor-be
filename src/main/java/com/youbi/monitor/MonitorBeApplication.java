package com.youbi.monitor;

import org.apache.ibatis.annotations.Mapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

@MapperScan(basePackages = "com.youbi.monitor.repository", annotationClass = Mapper.class)
@SpringBootApplication
@ComponentScan(excludeFilters = @ComponentScan.Filter(
        type = FilterType.REGEX,
        pattern = {
                "com\\.youbi\\.monitor\\.controller\\.(?:AccountOverviewController|BilibiliPlaywrightController|SocialPlaywrightInspectController|(?:Bilibili|Douyin|Xiaohongshu|Shipinhao|Kuaishou|Jinritoutiao)UploadController|(?:Douyin|Xiaohongshu|Shipinhao|Kuaishou|Jinritoutiao)AccountController)",
                "com\\.youbi\\.monitor\\.service\\.(?:AccountOverviewService|MonitorAsyncUploadService|BilibiliPlaywrightAccountService|SocialBrowserFactory|SocialHumanActions|SocialPlaywrightInspectService|SocialRiskDetector|(?:BilibiliPlaywright|Bilibili|Douyin|Xiaohongshu|Shipinhao|Kuaishou|Jinritoutiao)UploadService|(?:Douyin|Xiaohongshu|Shipinhao|Kuaishou|Jinritoutiao)AccountService)",
                "com\\.youbi\\.monitor\\.repository\\.(?:BilibiliPlaywrightAccountRepository|MonitorAsyncUploadRepository)",
                "com\\.youbi\\.monitor\\.repository\\.impl\\.(?:BilibiliPlaywrightAccountRepositoryServiceImpl|MonitorAsyncUploadRepositoryServiceImpl)"
        }
))
@EnableScheduling
public class MonitorBeApplication {
    public static void main(String[] args) {
        SpringApplication.run(MonitorBeApplication.class, args);
    }
}
