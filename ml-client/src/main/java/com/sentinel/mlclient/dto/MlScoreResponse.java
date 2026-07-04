package com.sentinel.mlclient.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MlScoreResponse(
        @JsonProperty("transaction_id") String transactionId,
        @JsonProperty("fraud_score") double fraudScore
) {}