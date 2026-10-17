package com.sentinel.mlclient.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 추론 서버 응답.
 * modelName / modelVersion / runId 는 이 점수를 실제로 낸 모델 (FRAUD_RESULTS.MODEL_ID 결정용).
 * 구버전 서버이거나 Fallback 이면 null.
 */
public record MlScoreResponse(
        @JsonProperty("transaction_id") String transactionId,
        @JsonProperty("fraud_score") double fraudScore,
        @JsonProperty("model_name") String modelName,
        @JsonProperty("model_version") String modelVersion,
        @JsonProperty("run_id") String runId
) {
    /** Fallback 등 모델 정보가 없는 응답 (기존 2인자 호출부 호환) */
    public MlScoreResponse(String transactionId, double fraudScore) {
        this(transactionId, fraudScore, null, null, null);
    }

    public boolean hasModelInfo() {
        return modelName != null && modelVersion != null && runId != null;
    }
}
