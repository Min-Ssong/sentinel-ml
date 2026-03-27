package com.sentinel.domain.model;

/**
 * 최종 이상거래 탐지 결과
 * Rule Engine 결과 + ML 점수를 합산하여 최종 판단
 */
public record FraudDetectionResult(
        String transactionId,
        DetectionStatus status,     // BLOCKED / REVIEW / APPROVED
        String source,              // "RULE" or "ML" or "COMBINED"
        double finalScore,          // 최종 위험도 0.0 ~ 1.0
        String reason               // 판단 근거
) {
    public enum DetectionStatus {
        BLOCKED,   // 즉시 차단 (rule score = 1.0 또는 ML score > 0.9)
        REVIEW,    // 검토 필요 (ML score 0.5 ~ 0.9)
        APPROVED   // 정상 승인 (ML score < 0.5)
    }
}