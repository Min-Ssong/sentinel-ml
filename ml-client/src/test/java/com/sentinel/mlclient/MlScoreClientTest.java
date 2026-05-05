package com.sentinel.mlclient;

import com.sentinel.mlclient.dto.MlScoreRequest;
import com.sentinel.mlclient.dto.MlScoreResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class MlScoreClientTest {

    private final MlScoreClient mlScoreClient = new MlScoreClient("http://localhost:8000");

    private MlScoreRequest sampleRequest() {
        return new MlScoreRequest(
                "txId-001", "user-001", new BigDecimal("10000"),
                "KR", "ONLINE", "device-1", "1.1.1.1"
        );
    }

    @Test
    @DisplayName("ML 서버 다운 시 Fallback은 score -1.0을 반환한다")
    void score_shouldFallback_whenMlServerDown() {
        MlScoreResponse response = mlScoreClient.fallback(
                sampleRequest(), new RuntimeException("Connection refused"));

        assertThat(response.score()).isEqualTo(-1.0);
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