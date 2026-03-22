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
            TransactionMessage message = isFraud ? buildFraudTransaction() : buildNormalTransaction();
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

    private TransactionMessage buildFraudTransaction() {
        // 일단 정상과 동일하게 생성, 다음 단계에서 이상 패턴 3가지로 분리
        return new TransactionMessage(
                UUID.randomUUID().toString(),
                USER_IDS.get(random.nextInt(USER_IDS.size())),
                BigDecimal.valueOf(5000000 + random.nextInt(5000000)),  // 500만 ~ 1000만 (비정상 고액)
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                "device-" + random.nextInt(10),
                "192.168.1." + random.nextInt(255),
                COUNTRIES.get(random.nextInt(COUNTRIES.size())),
                LocalDateTime.now(),
                true
        );
    }
}