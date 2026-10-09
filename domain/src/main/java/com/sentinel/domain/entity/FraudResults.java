package com.sentinel.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;

/**
 * 모델 판단 결과. 거래 1건에 CHAMPION 1행 + (Shadow 활성 시) CHALLENGER 1행 (N:1).
 * ML 이 점수를 낸 거래만 저장한다 — 룰 차단·Fallback 은 판단한 모델이 없어 MODEL_ID(NOT NULL)를 채울 수 없음.
 */
@Entity
@Table(name = "fraud_results")
@EntityListeners(AuditingEntityListener.class)
@Getter
public class FraudResults {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "transaction_id", nullable = false)
    private Transaction transaction;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "model_id", nullable = false)
    private ModelMetadata model;

    @Column(length = 100)
    private String ruleTriggered;

    @Column(precision = 5, scale = 4)
    private BigDecimal mlScore;

    @Column(nullable = false, length = 20)
    private String finalDecision;

    @Enumerated(EnumType.STRING)
    @Column(name = "role", nullable = false, length = 20)
    private ModelRole role;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    protected FraudResults() {
    }

    public static FraudResults of(Transaction transaction, ModelMetadata model, ModelRole role,
                                  double mlScore, String finalDecision) {
        FraudResults r = new FraudResults();
        r.transaction = transaction;
        r.model = model;
        r.role = role;
        // DECIMAL(5,4): 점수는 0~1 이므로 소수 4자리 반올림
        r.mlScore = BigDecimal.valueOf(mlScore).setScale(4, RoundingMode.HALF_UP);
        r.finalDecision = finalDecision;
        return r;
    }
}
