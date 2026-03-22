package com.sentinelml.simulator.dto;

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
        boolean isFraud          // 시뮬레이터 전용 필드, 검증용
) {}