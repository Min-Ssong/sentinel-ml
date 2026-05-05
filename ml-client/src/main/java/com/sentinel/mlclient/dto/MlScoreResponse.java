package com.sentinel.mlclient.dto;

public record MlScoreResponse(
        String transactionId,
        double score  // 0.0 ~ 1.0
) {}