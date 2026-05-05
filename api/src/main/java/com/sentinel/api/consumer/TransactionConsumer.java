package com.sentinel.api.consumer;

import com.sentinel.common.dto.TransactionMessage;
import com.sentinel.domain.entity.Transaction;
import com.sentinel.api.service.TransactionService;
import com.sentinel.api.service.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);
    private final TransactionService transactionService;
    private final RedisService redisService;

    public TransactionConsumer(TransactionService transactionService, RedisService redisService) {
        this.transactionService = transactionService;
        this.redisService = redisService;
    }

    @KafkaListener(topics = "transactions", groupId = "sentinel-group")
    public void consume(TransactionMessage message) {
        try {
            MDC.put("txId", message.transactionId());
            log.info("Transaction received | userId={} | amount={} | isFraud={}",
                    message.userId(), message.amount(), message.isFraud());

            // Redis 거래 이력 저장
            redisService.saveTransactionHistory(message.userId(), message.transactionId());

            // IP/기기 카운터 증가
            redisService.incrementCounter("ip:" + message.ipAddress() + ":count");
            redisService.incrementCounter("device:" + message.deviceId() + ":count");

            Transaction transaction = toEntity(message);
            transactionService.save(transaction);

        } catch (Exception e) {
            log.error("Transaction processing failed | txId={} | error={}",
                    message.transactionId(), e.getMessage());
            throw e;
        } finally {
            MDC.clear();
        }
    }

    private Transaction toEntity(TransactionMessage message) {
        Transaction transaction = new Transaction();
        transaction.setTransactionId(message.transactionId());
        transaction.setUserId(message.userId());
        transaction.setAmount(message.amount());
        transaction.setMerchantCategory(message.merchantCategory());
        transaction.setDeviceId(message.deviceId());
        transaction.setIpAddress(message.ipAddress());
        transaction.setCountry(message.country());
        transaction.setTimestamp(message.timestamp());
        transaction.setStatus("PENDING");
        return transaction;
    }
}