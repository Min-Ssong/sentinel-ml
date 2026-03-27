package com.sentinelml.simulator;

import com.sentinelml.simulator.config.SimulatorProperties;
import com.sentinelml.simulator.dto.TransactionMessage;
import com.sentinelml.simulator.producer.TransactionProducer;
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
    private final Random random = new Random();

    // 시뮬레이션용 고정 풀
    private static final List<String> USER_IDS = List.of(
            "user-001", "user-002", "user-003", "user-004", "user-005"
    );
    private static final List<String> CATEGORIES = List.of(
            "GROCERY", "RESTAURANT", "ONLINE", "GAS_STATION", "TRAVEL"
    );
    private static final List<String> COUNTRIES = List.of(
            "KR", "US", "JP"
    );

    public SimulatorRunner(TransactionProducer producer, SimulatorProperties properties) {
        this.producer = producer;
        this.properties = properties;
    }

    @Override
    public void run(String... args) throws Exception {
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
                USER_IDS.get(random.nextInt(USER_IDS.size())),
                BigDecimal.valueOf(1000 + random.nextInt(490000)),  // 1천 ~ 50만
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168.1." + random.nextInt(255),
                "KR",
                LocalDateTime.now(),
                false
        );
    }

    // 패턴 선택기
    private TransactionMessage buildFraudTransaction() {
        int pattern = random.nextInt(3);
        return switch (pattern) {
            case 0 -> buildHighFrequencyFraud();
            case 1 -> buildHighAmountFraud();
            default -> buildLocationMismatchFraud();
        };
    }

    // 패턴 1: 단시간 고빈도 — 동일 userId로 짧은 간격 반복
    private TransactionMessage buildHighFrequencyFraud() {
        String fixedUser = "user-001";  // 동일 userId 고정
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                fixedUser,
                BigDecimal.valueOf(10000 + random.nextInt(50000)),
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(3),
                "192.168.1." + random.nextInt(255),
                "KR",
                LocalDateTime.now(),
                true
        );
    }

    // 패턴 2: 비정상 고액 — 정상 거래 상한(50만)의 10배 이상
    private TransactionMessage buildHighAmountFraud() {
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                USER_IDS.get(random.nextInt(USER_IDS.size())),
                BigDecimal.valueOf(5000000 + random.nextInt(5000000)),  // 500만~1000만
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168.1." + random.nextInt(255),
                "KR",
                LocalDateTime.now(),
                true
        );
    }

    // 패턴 3: 이상 위치 — KR에서 방금 결제했는데 해외 국가에서 결제
    private TransactionMessage buildLocationMismatchFraud() {
        List<String> foreignCountries = List.of("US", "JP", "CN", "RU");
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                USER_IDS.get(random.nextInt(USER_IDS.size())),
                BigDecimal.valueOf(100000 + random.nextInt(400000)),
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168." + random.nextInt(255) + "." + random.nextInt(255),
                foreignCountries.get(random.nextInt(foreignCountries.size())),
                LocalDateTime.now(),
                true
        );
    }
}