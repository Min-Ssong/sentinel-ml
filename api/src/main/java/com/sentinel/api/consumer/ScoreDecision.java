package com.sentinel.api.consumer;

/**
 * 점수 → 판정·예측 결과 규칙. 챔피언과 Shadow 가 같은 기준을 쓰도록 한곳에 둔다.
 * (Shadow 의 판정은 기록용이며 실제 거래 상태에 쓰이지 않음)
 */
final class ScoreDecision {

    static final double BLOCK_THRESHOLD = 0.8;
    static final double REVIEW_THRESHOLD = 0.5;
    // 학습 시 predict() 기본 임계값과 동일. 운영 지표를 오프라인 성능과 비교하기 위한 기준
    static final double PREDICTION_THRESHOLD = 0.5;

    private ScoreDecision() {
    }

    static String decide(double score) {
        if (score >= BLOCK_THRESHOLD) {
            return "BLOCKED";
        }
        if (score >= REVIEW_THRESHOLD) {
            return "REVIEW";
        }
        return "APPROVED";
    }

    /** tp / fp / fn / tn */
    static String outcome(double score, boolean actualFraud) {
        boolean predictedFraud = score >= PREDICTION_THRESHOLD;
        return predictedFraud
                ? (actualFraud ? "tp" : "fp")
                : (actualFraud ? "fn" : "tn");
    }
}
