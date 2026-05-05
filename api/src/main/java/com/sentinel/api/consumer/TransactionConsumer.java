package com.sentinel.api.consumer;

import com.sentinel.common.dto.TransactionMessage;
import com.sentinel.domain.entity.Transaction;
import com.sentinel.domain.rule.FraudScoreResult;
import com.sentinel.domain.rule.RuleEvaluator;
import com.sentinel.api.service.TransactionService;
import com.sentinel.api.service.RedisService;
import com.sentinel.mlclient.MlScoreClient;
import com.sentinel.mlclient.dto.MlScoreRequest;
import com.sentinel.mlclient.dto.MlScoreResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);
    private static final double BLOCK_THRESHOLD = 0.8;
    private static final double REVIEW_THRESHOLD = 0.5;

    private final TransactionService transactionService;
    private final RedisService redisService;
    private final RuleEvaluator ruleEvaluator;
    private final MlScoreClient mlScoreClient;

    public TransactionConsumer(TransactionService transactionService,
                               RedisService redisService,
                               RuleEvaluator ruleEvaluator,
                               MlScoreClient mlScoreClient) {
        this.transactionService = transactionService;
        this.redisService = redisService;
        this.ruleEvaluator = ruleEvaluator;
        this.mlScoreClient = mlScoreClient;
    }

    @KafkaListener(topics = "transactions", groupId = "sentinel-group")
    public void consume(TransactionMessage message) {
        try {
            MDC.put("txId", message.transactionId());
            log.info("Transaction received | userId={} | amount={} | isFraud={}",
                    message.userId(), message.amount(), message.isFraud());

            // Redis 거래 이력 저장
            redisService.saveTransactionHistory(message.userId(), message.transactionId());
            redisService.incrementCounter("ip:" + message.ipAddress() + ":count");
            redisService.incrementCounter("device:" + message.deviceId() + ":count");

            // Rule Engine 평가
            FraudScoreResult ruleResult = ruleEvaluator.evaluate(
                    message.transactionId(),
                    message.userId(),
                    message.amount(),
                    message.country(),
                    message.deviceId(),
                    message.ipAddress()
            );

            String status;

            if (ruleResult.blocked()) {
                // Rule Engine 차단 → ML 호출 불필요
                status = "BLOCKED";
                log.info("Transaction blocked by rule | txId={} | rule={} | reason={}",
                        message.transactionId(), ruleResult.triggeredRule(), ruleResult.reason());
            } else {
                // Rule Engine 통과 → ML 서버 호출
                MlScoreRequest mlRequest = new MlScoreRequest(
                        message.transactionId(),
                        message.userId(),
                        message.amount(),
                        message.country(),
                        message.merchantCategory(),
                        message.deviceId(),
                        message.ipAddress()
                );

                MlScoreResponse mlResponse = mlScoreClient.score(mlRequest);

                if (mlResponse.score() < 0) {
                    // Fallback 발생 (ML 서버 다운)
                    status = "PENDING";
                    log.warn("ML fallback applied | txId={} | status=PENDING",
                            message.transactionId());
                } else if (mlResponse.score() >= BLOCK_THRESHOLD) {
                    status = "BLOCKED";
                } else if (mlResponse.score() >= REVIEW_THRESHOLD) {
                    status = "REVIEW";
                } else {
                    status = "APPROVED";
                }

                log.info("ML score applied | txId={} | score={} | status={}",
                        message.transactionId(), mlResponse.score(), status);
            }

            Transaction transaction = toEntity(message, status);
            transactionService.save(transaction);

        } catch (Exception e) {
            log.error("Transaction processing failed | txId={} | error={}",
                    message.transactionId(), e.getMessage());
            throw e;
        } finally {
            MDC.clear();
        }
    }

    private Transaction toEntity(TransactionMessage message, String status) {
        Transaction transaction = new Transaction();
        transaction.setTransactionId(message.transactionId());
        transaction.setUserId(message.userId());
        transaction.setAmount(message.amount());
        transaction.setMerchantCategory(message.merchantCategory());
        transaction.setDeviceId(message.deviceId());
        transaction.setIpAddress(message.ipAddress());
        transaction.setCountry(message.country());
        transaction.setTimestamp(message.timestamp());
        transaction.setStatus(status);
        return transaction;
    }
}