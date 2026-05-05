package com.sentinel.mlclient;

import com.sentinel.mlclient.dto.MlScoreRequest;
import com.sentinel.mlclient.dto.MlScoreResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

@Component
public class MlScoreClient {

    private static final Logger log = LoggerFactory.getLogger(MlScoreClient.class);
    private static final String CIRCUIT_BREAKER_NAME = "mlScoreClient";

    private final WebClient webClient;

    public MlScoreClient(@Value("${ml.server.url:http://localhost:8000}") String mlServerUrl) {
        this.webClient = WebClient.builder()
                .baseUrl(mlServerUrl)
                .build();
    }

    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "fallback")
    public MlScoreResponse score(MlScoreRequest request) {
        return webClient.post()
                .uri("/score")
                .header("X-Transaction-ID", request.transactionId())
                .bodyValue(request)
                .retrieve()
                .bodyToMono(MlScoreResponse.class)
                .timeout(Duration.ofMillis(50))
                .block();
    }

    public MlScoreResponse fallback(MlScoreRequest request, Throwable t) {
        log.warn("ML server fallback triggered | txId={} | reason={}",
                request.transactionId(), t.getMessage());
        return new MlScoreResponse(request.transactionId(), -1.0); // -1.0 = ML 호출 실패 표시
    }
}