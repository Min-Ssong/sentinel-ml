package com.sentinel.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.Instant;

/**
 * 거래의 정답 라벨 (사기 여부). Champion/Challenger 비교 리포트의 기준값.
 *
 * - 거래 테이블과 분리한 이유: 실제 금융권에서는 확정 라벨(차지백·고객 신고 등)이 거래보다 늦게 도착한다.
 *   지금은 시뮬레이터가 메시지에 정답을 실어 보내 즉시 저장하지만(LABEL_SOURCE = SIMULATOR),
 *   테이블 구조는 지연 도착을 표현할 수 있게 둔다. 운영 거래 테이블에 시뮬레이션 전용 값을 섞지 않는다.
 * - 룰 차단·Fallback 거래도 저장한다 (모델 판단 여부와 무관한 거래 단위 정답).
 * - LABELED_AT 은 TIMESTAMPTZ + Instant (2026-10-17). 다른 테이블의 시각 컬럼(TIMESTAMP + LocalDateTime, KST)도
 *   같은 방식으로 옮길 예정 (R2).
 */
@Entity
@Table(name = "transaction_labels")
@Getter
public class TransactionLabel {

    public static final String SOURCE_SIMULATOR = "SIMULATOR";

    @Id
    private String transactionId;

    // PK = 거래 ID (공유 키). 거래 행이 먼저 INSERT 되도록 연관관계로 매핑
    @MapsId
    @OneToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "transaction_id")
    private Transaction transaction;

    @Column(name = "is_fraud", nullable = false)
    private boolean fraud;

    @Column(name = "label_source", nullable = false, length = 20)
    private String labelSource;

    // columnDefinition: ddl-auto=validate 에서 timestamptz 컬럼과 타입명으로 비교되게 함
    @Column(name = "labeled_at", nullable = false, columnDefinition = "timestamptz")
    private Instant labeledAt;

    protected TransactionLabel() {
    }

    public static TransactionLabel of(Transaction transaction, boolean fraud, String labelSource) {
        TransactionLabel l = new TransactionLabel();
        l.transaction = transaction;
        l.fraud = fraud;
        l.labelSource = labelSource;
        l.labeledAt = Instant.now();
        return l;
    }
}
