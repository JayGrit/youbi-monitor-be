package com.youbi.monitor.model;

import java.util.Map;

public record UploaderPhoneRecord(
        Long id,
        String phone,
        String remark,
        String note,
        Map<String, Long> accounts,
        Map<String, UploaderPhoneBinding> bindings
) {
}
