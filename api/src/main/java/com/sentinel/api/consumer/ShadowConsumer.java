package com.sentinel.api.consumer;

import com.sentinel.api.service.ModelMetadataService;
import com.sentinel.api.service.TransactionService;
import com.sentinel.common.dto.TransactionMessage;
import com.sentinel.mlclient.ChallengerScoreClient;
import com.sentinel.mlclient.dto.MlScoreRequest;
import com.sentinel.mlclient.dto.MlScoreResponse;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Shadow Deployment — 챌린저 모델 병행 추론.
 *
 * 같은 transactions 토픽을 별도 consumer group 으로 읽어 챌린저 서빙에만 점수를 요청하고,
 *  1) 예측 결과를 별도 지표(sentinel.ml.shadow.predictions)로 집계하고
 *  2) FRAUD_RESULTS 에 ROLE=CHALLENGER 로 저장한다.
 *
 * 운영 경로와 분리하는 규칙
 *  - Redis 이력·카운터를 건드리지 않는다 (같이 올리면 고빈도 룰이 두 배로 걸려 운영 판정이 오염됨)
 *  - 룰 평가·상태 결정·Transaction 저장을 하지 않는다 (판정은 챔피언만)
 *  - 예외를 다시 던지지 않는다. 전용 컨테이너 팩토리도 재시도·DLQ 없음 (운영 DLQ 오염 방지)
 *  - auto.offset.reset=latest: 처음 붙을 때 토픽의 과거 이력을 재처리하지 않음
 *
 * 외래키 경쟁 (DB1): FRAUD_RESULTS.TRANSACTION_ID 는 TRANSACTIONS 를 참조하는데, 두 consumer group 은 독립이라
 * 챔피언이 거래를 저장하기 전에 Shadow 가 먼저 올 수 있다. 거래 행이 생길 때까지 짧게 기다리고,
 * 시간 안에 생기지 않으면 저장을 생략하고 sentinel.ml.shadow.save_skipped 로 센다.
 *
 * shadow.enabled=true 일 때만 빈이 생성된다 (기본 비활성 → 운영 영향 없음, 끄면 즉시 롤백).
 */
@Component
@ConditionalOnProperty(name = "shadow.enabled", havingValue = "true")
public class ShadowConsumer {

    private static final Logger log = LoggerFactory.getLogger(ShadowConsumer.class);

    private final ChallengerScoreClient challengerScoreClient;
    private final MeterRegistry meterRegistry;
    private final ModelMetadataService modelMetadataService;
    private final TransactionService transactionService;
    private final long waitIntervalMs;
    private final long maxWaitMs;

    public ShadowConsumer(ChallengerScoreClient challengerScoreClient,
                          MeterRegistry meterRegistry,
                          ModelMetadataService modelMetadataService,
                          TransactionService transactionService,
                          @Value("${shadow.result.wait-interval-ms:50}") long waitIntervalMs,
                          @Value("${shadow.result.max-wait-ms:2000}") long maxWaitMs) {
        this.challengerScoreClient = challengerScoreClient;
        this.meterRegistry = meterRegistry;
        this.modelMetadataService = modelMetadataService;
        this.transactionService = transactionService;
        this.waitIntervalMs = waitIntervalMs;
        this.maxWaitMs = maxWaitMs;
    }

    @KafkaListener(topics = "transactions",
            groupId = "sentinel-shadow-group",
            containerFactory = "shadowKafkaListenerContainerFactory",
            properties = {"auto.offset.reset=latest"})
    public void consume(TransactionMessage message) {
        try {
            MDC.put("txId", message.transactionId());

            MlScoreResponse response = challengerScoreClient.score(
                    new MlScoreRequest(message.transactionId(), message.features()));
            double score = response.fraudScore();

            if (score < 0) {
                // 챌린저 서빙 장애·타임아웃 (Circuit Breaker fallback)
                meterRegistry.counter("sentinel.ml.shadow.fallback").increment();
                return;
            }
            meterRegistry.counter("sentinel.ml.shadow.predictions",
                    "outcome", ScoreDecision.outcome(score, message.isFraud())).increment();

            saveResult(message.transactionId(), response, score);

        } catch (Exception e) {
            // Shadow 실패는 운영에 영향을 주지 않도록 기록만 하고 넘긴다
            meterRegistry.counter("sentinel.ml.shadow.error").increment();
            log.warn("Shadow processing failed | txId={} | error={}",
                    message.transactionId(), e.getMessage());
        } finally {
            MDC.clear();
        }
    }

    private void saveResult(String transactionId, MlScoreResponse response, double score)
            throws InterruptedException {
        Optional<Long> modelId = modelMetadataService.resolveId(
                response.modelName(), response.modelVersion(), response.runId());
        if (modelId.isEmpty()) {
            meterRegistry.counter("sentinel.ml.result.skipped", "role", "challenger").increment();
            return;
        }
        if (!waitForTransaction(transactionId)) {
            meterRegistry.counter("sentinel.ml.shadow.save_skipped").increment();
            log.warn("Shadow result not saved: transaction row not found within {}ms | txId={}",
                    maxWaitMs, transactionId);
            return;
        }
        transactionService.saveChallengerResult(
                transactionId, modelId.get(), score, ScoreDecision.decide(score));
    }

    /** 챔피언이 거래를 저장할 때까지 짧게 기다린다. 기다린 시간은 지표로 남겨 처리량 영향을 본다. */
    private boolean waitForTransaction(String transactionId) throws InterruptedException {
        long start = System.nanoTime();
        long deadline = start + maxWaitMs * 1_000_000L;
        while (true) {
            if (transactionService.exists(transactionId)) {
                meterRegistry.summary("sentinel.ml.shadow.fk_wait_ms")
                        .record((System.nanoTime() - start) / 1_000_000.0);
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(waitIntervalMs);
        }
    }
}
