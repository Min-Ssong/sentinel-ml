package com.sentinel.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 시각 컬럼은 TIMESTAMPTZ + Instant (2026-10-18, R2).
 * 이전에는 TIMESTAMP + LocalDateTime 이라 KST 벽시계 값이 시간대 없이 저장되고, DB 세션(UTC)의 now() 기준 쿼리가 9시간 어긋났다.
 */
@Entity
@Table(name = "transactions")
@EntityListeners(AuditingEntityListener.class)
@Getter
@Setter
public class Transaction {

    @Id
    @Column(name = "transaction_id", length = 36, nullable = false)
    private String transactionId;

    @Column(name = "user_id", length = 36, nullable = false)
    private String userId;

    @Column(name = "amount", nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    @Column(name = "merchant_category", length = 50, nullable = false)
    private String merchantCategory;

    @Column(name = "device_id", length = 100)
    private String deviceId;

    @Column(name = "ip_address", length = 45)
    private String ipAddress;

    @Column(name = "country", length = 10)
    private String country;

    // 거래 발생 시각 (메시지의 timestamp). columnDefinition: ddl-auto=validate 에서 timestamptz 와 비교되게 함
    @Column(name = "timestamp", nullable = false, columnDefinition = "timestamptz")
    private Instant timestamp;

    @Column(name = "status", length = 20, nullable = false)
    private String status;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false, columnDefinition = "timestamptz")
    private Instant createdAt;
}
