package com.youbi.monitor.repository;

import com.youbi.monitor.service.MonitorService;

import java.util.List;

public interface ISubmitterAuthorRepositoryService {
    MonitorService.SubmitterAuthorType findSubmitterAuthorType(String author);

    List<MonitorService.SubmitterAuthorType> listSubmitterAuthorTypes();

    MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            String author,
            String type,
            String taskType,
            Boolean hasBackgroundAudio,
            String sourceLanguage,
            String targetLanguage,
            Boolean resetCover,
            String coverOrientation,
            Boolean fetchNewVideos
    );

    MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(String author);
}
