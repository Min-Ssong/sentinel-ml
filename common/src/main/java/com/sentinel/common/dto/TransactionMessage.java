package com.sentinel.common.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record TransactionMessage(
        String transactionId,
        String userId,
        BigDecimal amount,
        String merchantCategory,
        String deviceId,
        String ipAddress,
        String country,
        LocalDateTime timestamp,
        boolean isFraud,
        List<Double> features   // ML 입력 30개: Time, V1~V28, Amount
) {}