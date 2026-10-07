package com.sentinel.api.consumer;

import com.sentinel.common.dto.TransactionMessage;
import com.sentinel.mlclient.ChallengerScoreClient;
import com.sentinel.mlclient.dto.MlScoreRequest;
import com.sentinel.mlclient.dto.MlScoreResponse;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Shadow Deployment — 챌린저 모델 병행 추론.
 *
 * 같은 transactions 토픽을 별도 consumer group 으로 읽어 챌린저 서빙에만 점수를 요청하고,
 * 예측 결과를 별도 지표(sentinel.ml.shadow.predictions)로 집계한다.
 *
 * 운영 경로와 분리하는 규칙
 *  - Redis 이력·카운터를 건드리지 않는다 (같이 올리면 고빈도 룰이 두 배로 걸려 운영 판정이 오염됨)
 *  - 룰 평가·상태 결정·Transaction 저장을 하지 않는다 (판정은 챔피언만)
 *  - 예외를 다시 던지지 않는다. 전용 컨테이너 팩토리도 재시도·DLQ 없음 (운영 DLQ 오염 방지)
 *  - auto.offset.reset=latest: 처음 붙을 때 토픽의 과거 이력을 재처리하지 않음
 *
 * shadow.enabled=true 일 때만 빈이 생성된다 (기본 비활성 → 운영 영향 없음, 끄면 즉시 롤백).
 */
@Component
@ConditionalOnProperty(name = "shadow.enabled", havingValue = "true")
public class ShadowConsumer {

    private static final Logger log = LoggerFactory.getLogger(ShadowConsumer.class);
    // TransactionConsumer.recordOutcome 과 같은 기준 (같은 조건으로 비교하기 위함)
    private static final double PREDICTION_THRESHOLD = 0.5;

    private final ChallengerScoreClient challengerScoreClient;
    private final MeterRegistry meterRegistry;

    public ShadowConsumer(ChallengerScoreClient challengerScoreClient, MeterRegistry meterRegistry) {
        this.challengerScoreClient = challengerScoreClient;
        this.meterRegistry = meterRegistry;
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
            recordOutcome(score, message.isFraud());
            log.debug("Shadow score | txId={} | score={}", message.transactionId(), score);

        } catch (Exception e) {
            // Shadow 실패는 운영에 영향을 주지 않도록 기록만 하고 넘긴다
            meterRegistry.counter("sentinel.ml.shadow.error").increment();
            log.warn("Shadow processing failed | txId={} | error={}",
                    message.transactionId(), e.getMessage());
        } finally {
            MDC.clear();
        }
    }

    private void recordOutcome(double fraudScore, boolean actualFraud) {
        boolean predictedFraud = fraudScore >= PREDICTION_THRESHOLD;
        String outcome = predictedFraud
                ? (actualFraud ? "tp" : "fp")
                : (actualFraud ? "fn" : "tn");
        meterRegistry.counter("sentinel.ml.shadow.predictions", "outcome", outcome).increment();
    }
}
