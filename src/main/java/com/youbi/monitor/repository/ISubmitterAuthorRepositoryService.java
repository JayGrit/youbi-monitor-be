package com.youbi.monitor.repository;

import com.youbi.monitor.service.MonitorService;

import java.util.List;

public interface ISubmitterAuthorRepositoryService {
    MonitorService.SubmitterAuthorType findSubmitterAuthorType(String author);

    List<MonitorService.SubmitterAuthorType> listSubmitterAuthorTypes();

    MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            String author,
            String type,
            Boolean needSubtitle,
            Boolean needDubbing,
            Boolean needSeparation,
            String sourceLanguage,
            String targetLanguage,
            Boolean resetCover
    );

    MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(String author);
}
