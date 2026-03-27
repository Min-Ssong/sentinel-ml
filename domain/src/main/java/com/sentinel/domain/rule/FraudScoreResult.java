package com.sentinel.domain.rule;

/**
 * Rule Engine 평가 결과
 * ML 호출 전 1차 판단 결과를 담는다.
 */
public record FraudScoreResult(
        String transactionId,
        boolean blocked,          // true면 즉시 차단, ML 호출 불필요
        String triggeredRule,     // 어떤 규칙에 걸렸는지 (예: "HIGH_FREQUENCY")
        double ruleScore,         // 규칙 기반 위험도 0.0 ~ 1.0
        String reason             // 차단 사유 설명
) {
    // 정상 통과 (ML 호출 필요)
    public static FraudScoreResult pass(String transactionId) {
        return new FraudScoreResult(transactionId, false, null, 0.0, null);
    }

    // 즉시 차단
    public static FraudScoreResult block(String transactionId, String rule, String reason) {
        return new FraudScoreResult(transactionId, true, rule, 1.0, reason);
    }
}