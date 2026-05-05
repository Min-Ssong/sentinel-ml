package com.sentinel.mlclient.dto;

import java.math.BigDecimal;

public record MlScoreRequest(
        String transactionId,
        String userId,
        BigDecimal amount,
        String country,
        String merchantCategory,
        String deviceId,
        String ipAddress
) {}