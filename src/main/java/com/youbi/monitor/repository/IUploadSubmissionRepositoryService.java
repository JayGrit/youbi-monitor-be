package com.youbi.monitor.repository;

import com.youbi.monitor.service.MonitorService;

import java.util.List;

public interface IUploadSubmissionRepositoryService {
    MonitorService.FailedUploadSubmissionList listFailedUploadSubmissions(String platform);

    MonitorService.UploadSubmissionRetryResult retryFailedUploadSubmissions(String platform, List<Long> ids);

    MonitorService.UploadBackfillCandidateList listUploadBackfillCandidates(String platform, String accountKey, String type);

    MonitorService.UploadBackfillRegisterResult registerUploadBackfill(String platform, String accountKey, String type, List<String> taskIds);
}
