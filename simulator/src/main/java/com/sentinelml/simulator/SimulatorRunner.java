package com.sentinelml.simulator;

import com.sentinelml.simulator.config.SimulatorProperties;
import com.sentinelml.simulator.loader.CreditCardFeatureLoader;
import com.sentinelml.simulator.producer.TransactionProducer;
import com.sentinel.common.dto.TransactionMessage;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Component
public class SimulatorRunner implements CommandLineRunner {

    private final TransactionProducer producer;
    private final SimulatorProperties properties;
    private final CreditCardFeatureLoader featureLoader;
    private final Random random = new Random();

    private static final int USER_POOL_SIZE = 1000;
    private static final List<String> USER_IDS = List.of(
            "user-001", "user-002", "user-003", "user-004", "user-005");
    private static final List<String> CATEGORIES = List.of(
            "GROCERY", "RESTAURANT", "ONLINE", "GAS_STATION", "TRAVEL");
    private static final List<String> COUNTRIES = List.of("KR", "US", "JP");

    public SimulatorRunner(TransactionProducer producer,
                           SimulatorProperties properties,
                           CreditCardFeatureLoader featureLoader) {
        this.producer = producer;
        this.properties = properties;
        this.featureLoader = featureLoader;
    }

    @Override
    public void run(String... args) throws Exception {
        featureLoader.load(properties.csvPath());   // csv 1회 로드

        System.out.printf("Simulator started | rate=%d/s | fraud-ratio=%.1f%%%n",
                properties.rate(), properties.fraudRatio() * 100);

        long intervalMs = 1000L / properties.rate();

        while (true) {
            boolean isFraud = random.nextDouble() < properties.fraudRatio();
            TransactionMessage message = isFraud
                    ? buildFraudTransaction()
                    : buildNormalTransaction();
            producer.send(message);
            Thread.sleep(intervalMs);
        }
    }

    private TransactionMessage buildNormalTransaction() {
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                randomUserId(),
                BigDecimal.valueOf(1000 + random.nextInt(490000)),
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168.1." + random.nextInt(255),
                "KR",
                LocalDateTime.now(),
                false,
                featureLoader.randomNormal()    // Class=0 행
        );
    }

    private TransactionMessage buildFraudTransaction() {
        int pattern = random.nextInt(3);
        return switch (pattern) {
            case 0 -> buildHighFrequencyFraud();
            case 1 -> buildHighAmountFraud();
            default -> buildLocationMismatchFraud();
        };
    }

    private TransactionMessage buildHighFrequencyFraud() {
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                "user-001",
                BigDecimal.valueOf(10000 + random.nextInt(50000)),
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(3),
                "192.168.1." + random.nextInt(255),
                "KR",
                LocalDateTime.now(),
                true,
                featureLoader.randomFraud()     // Class=1 행
        );
    }

    private TransactionMessage buildHighAmountFraud() {
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                randomUserId(),
                BigDecimal.valueOf(5000000 + random.nextInt(5000000)),
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168.1." + random.nextInt(255),
                "KR",
                LocalDateTime.now(),
                true,
                featureLoader.randomFraud()
        );
    }

    private TransactionMessage buildLocationMismatchFraud() {
        List<String> foreignCountries = List.of("US", "JP", "CN", "RU");
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                randomUserId(),
                BigDecimal.valueOf(100000 + random.nextInt(400000)),
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168." + random.nextInt(255) + "." + random.nextInt(255),
                foreignCountries.get(random.nextInt(foreignCountries.size())),
                LocalDateTime.now(),
                true,
                featureLoader.randomFraud()
        );
    }

    // %04d로 만들어 고빈도 사기용 "user-001"과 문자열이 겹치지 않게 함
    private String randomUserId() {
        return String.format("user-%04d", random.nextInt(USER_POOL_SIZE) + 1);
    }
}