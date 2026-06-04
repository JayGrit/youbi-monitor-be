package com.youbi.monitor.repository;

import com.youbi.monitor.dto.UploaderAccountState;

import java.util.Optional;

public interface IUploaderAccountRepositoryService {
    Optional<UploaderAccountState> state(String platform, String accountKey);
}
