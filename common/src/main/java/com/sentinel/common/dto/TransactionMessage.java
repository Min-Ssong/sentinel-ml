package com.sentinel.common.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Kafka 메시지 계약 (transactions 토픽).
 * timestamp 는 Instant (2026-10-18, R2. 이전 LocalDateTime). 생산자(시뮬레이터)와 소비자(API)를 함께 배포해야 한다.
 */
public record TransactionMessage(
        String transactionId,
        String userId,
        BigDecimal amount,
        String merchantCategory,
        String deviceId,
        String ipAddress,
        String country,
        Instant timestamp,
        boolean isFraud,
        List<Double> features   // ML 입력 30개: Time, V1~V28, Amount
) {}
