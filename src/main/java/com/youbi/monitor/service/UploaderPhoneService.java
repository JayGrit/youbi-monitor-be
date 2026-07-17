package com.youbi.monitor.service;

import com.youbi.monitor.dto.UploaderPhoneAccountUpdateRequest;
import com.youbi.monitor.model.UploaderPhoneMatrixResponse;
import com.youbi.monitor.model.UploaderPhoneRecord;
import com.youbi.monitor.repository.IUploaderPhoneRepositoryService;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class UploaderPhoneService {
    private final IUploaderPhoneRepositoryService repositoryService;

    public UploaderPhoneService(IUploaderPhoneRepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public UploaderPhoneMatrixResponse matrix() {
        return repositoryService.matrix();
    }

    public UploaderPhoneRecord updatePlatformAccount(long phoneId, String platform, UploaderPhoneAccountUpdateRequest request) throws IOException {
        return repositoryService.updatePlatformAccount(phoneId, platform, request);
    }
}
