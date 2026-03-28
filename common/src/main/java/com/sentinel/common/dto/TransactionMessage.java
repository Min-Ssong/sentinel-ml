package com.sentinel.common.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TransactionMessage(
        String transactionId,
        String userId,
        BigDecimal amount,
        String merchantCategory,
        String deviceId,
        String ipAddress,
        String country,
        LocalDateTime timestamp,
        boolean isFraud
) {}