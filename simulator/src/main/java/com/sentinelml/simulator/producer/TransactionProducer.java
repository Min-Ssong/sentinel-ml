package com.sentinelml.simulator.producer;

import org.springframework.kafka.core.KafkaTemplate;
import com.sentinel.common.dto.TransactionMessage;
import org.springframework.stereotype.Component;

@Component
public class TransactionProducer {

    private static final String TOPIC = "transactions";

    private final KafkaTemplate<String, TransactionMessage> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, TransactionMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(TransactionMessage message) {
        // 파티션 키: userId (동일 사용자 거래는 같은 파티션으로)
        kafkaTemplate.send(TOPIC, message.userId(), message);
    }
}