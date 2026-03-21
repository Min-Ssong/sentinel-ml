package com.sentinel.domain.entity;

import jakarta.persistence.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "fraud_results")
@EntityListeners(AuditingEntityListener.class)
public class FraudResults {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "transaction_id")
    private Transaction transactionId;

    @ManyToOne
    @JoinColumn(name = "model_id")
    private ModelMetadata modelId;

    @Column(length = 100)
    private String ruleTriggered;

    @Column(precision = 5, scale = 4)
    private BigDecimal mlScore;

    @Column(nullable = false, length = 20)
    private String finalDecision;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}