package com.sentinel.mlclient;

import com.sentinel.mlclient.dto.MlScoreRequest;
import com.sentinel.mlclient.dto.MlScoreResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MlScoreClientTest {

    private final MlScoreClient mlScoreClient = new MlScoreClient("http://localhost:8000");

    private MlScoreRequest sampleRequest() {
        List<Double> features = new ArrayList<>(Collections.nCopies(30, 0.0));
        return new MlScoreRequest("txId-001", features);
    }

    @Test
    @DisplayName("ML 서버 다운 시 Fallback은 score -1.0을 반환한다")
    void score_shouldFallback_whenMlServerDown() {
        MlScoreResponse response = mlScoreClient.fallback(
                sampleRequest(), new RuntimeException("Connection refused"));

        assertThat(response.fraudScore()).isEqualTo(-1.0);
        assertThat(response.transactionId()).isEqualTo("txId-001");
    }

    @Test
    @DisplayName("Fallback 응답의 transactionId는 요청과 동일하다")
    void fallback_shouldReturnSameTransactionId() {
        MlScoreResponse response = mlScoreClient.fallback(
                sampleRequest(), new RuntimeException("Timeout"));

        assertThat(response.transactionId()).isEqualTo("txId-001");
    }
}