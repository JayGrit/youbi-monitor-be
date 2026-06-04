package com.youbi.monitor.repository;

import com.youbi.monitor.dto.UploaderPhoneAccountUpdateRequest;
import com.youbi.monitor.model.UploaderPhoneMatrixResponse;
import com.youbi.monitor.model.UploaderPhoneRecord;

import java.io.IOException;

public interface IUploaderPhoneRepositoryService {
    void ensureSchema();

    UploaderPhoneMatrixResponse matrix();

    UploaderPhoneRecord updatePlatformAccount(long phoneId, String platform, UploaderPhoneAccountUpdateRequest request) throws IOException;
}
