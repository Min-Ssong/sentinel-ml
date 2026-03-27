package com.sentinel.domain.rule;

import com.sentinel.domain.rule.FraudScoreResult;

/**
 * 규칙 기반 1차 이상거래 필터
 * Redis만 사용하므로 10ms 이내 처리가 목표
 * ML 호출 최소화를 위한 명백한 이상 패턴 즉시 차단
 */
public interface RuleEvaluator {

    /**
     * 거래에 대해 규칙을 평가한다.
     *
     * @param transactionId 거래 ID (MDC txId와 동일)
     * @param userId        사용자 ID
     * @param amount        거래 금액
     * @param country       거래 발생 국가
     * @param deviceId      기기 ID
     * @param ipAddress     IP 주소
     * @return 평가 결과 (blocked=true면 ML 호출 생략)
     */
    FraudScoreResult evaluate(
            String transactionId,
            String userId,
            java.math.BigDecimal amount,
            String country,
            String deviceId,
            String ipAddress
    );
}