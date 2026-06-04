package com.youbi.monitor.service;

import com.youbi.monitor.repository.IUploaderAccountRepositoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class UploaderAccountDailyResetScheduler {
    private static final Logger log = LoggerFactory.getLogger(UploaderAccountDailyResetScheduler.class);

    private final IUploaderAccountRepositoryService uploaderAccountRepositoryService;

    public UploaderAccountDailyResetScheduler(IUploaderAccountRepositoryService uploaderAccountRepositoryService) {
        this.uploaderAccountRepositoryService = uploaderAccountRepositoryService;
    }

    @Scheduled(cron = "0 0 5 * * *", zone = "Asia/Shanghai")
    public void resetTodayUploadCounts() {
        int updated = uploaderAccountRepositoryService.resetTodayUploadCounts();
        log.info("Reset uploader_account.today_upload_count rows={}", updated);
    }
}
