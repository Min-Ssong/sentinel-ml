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

/**
 * Shadow Deployment 용 챌린저 서빙(FastAPI, @challenger alias) 호출 클라이언트.
 *
 * MlScoreClient 와 같은 방식이지만 Circuit Breaker 이름을 분리했다.
 * 같은 클래스로 빈을 하나 더 만들면 "mlScoreClient" Circuit Breaker 상태를 공유해,
 * 챌린저 장애가 챔피언(운영) 호출까지 차단할 수 있기 때문이다.
 */
@Component
public class ChallengerScoreClient {

    private static final Logger log = LoggerFactory.getLogger(ChallengerScoreClient.class);
    private static final String CIRCUIT_BREAKER_NAME = "challengerScoreClient";

    private final WebClient webClient;
    private final long timeoutMs;

    public ChallengerScoreClient(
            @Value("${ml.challenger.url:http://localhost:8496}") String challengerUrl,
            @Value("${ml.challenger.timeout-ms:${ml.server.timeout-ms:50}}") long timeoutMs) {
        this.webClient = WebClient.builder().baseUrl(challengerUrl).build();
        this.timeoutMs = timeoutMs;
    }

    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "fallback")
    public MlScoreResponse score(MlScoreRequest request) {
        return webClient.post()
                .uri("/predict")
                .header("X-Transaction-ID", request.transactionId())
                .bodyValue(request)
                .retrieve()
                .bodyToMono(MlScoreResponse.class)
                .timeout(Duration.ofMillis(timeoutMs))
                .block();
    }

    public MlScoreResponse fallback(MlScoreRequest request, Throwable t) {
        log.warn("Challenger server fallback triggered | txId={} | reason={}",
                request.transactionId(), t.getMessage());
        return new MlScoreResponse(request.transactionId(), -1.0);
    }
}
