package com.sentinel.mlclient.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record MlScoreRequest(
        @JsonProperty("transaction_id") String transactionId,
        @JsonProperty("features") List<Double> features
) {}