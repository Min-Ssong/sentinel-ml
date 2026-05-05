package com.sentinel.domain.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalTime;

@Component
public class RedisRuleEvaluator implements RuleEvaluator {

    private static final Logger log = LoggerFactory.getLogger(RedisRuleEvaluator.class);
    private static final long WINDOW_MS = 60_000L;
    private static final int HIGH_FREQUENCY_THRESHOLD = 5;
    private static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("1000000");

    private final RedisTemplate<String, String> redisTemplate;

    public RedisRuleEvaluator(@Qualifier("redisTemplate") RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public FraudScoreResult evaluate(String transactionId, String userId,
                                     BigDecimal amount, String country, String deviceId, String ipAddress) {

        long start = System.currentTimeMillis();

        try {
            // 규칙 1: 단시간 고빈도
            FraudScoreResult highFreq = checkHighFrequency(transactionId, userId);
            if (highFreq.blocked()) return highFreq;

            // 규칙 2: 심야 고액 해외 결제
            FraudScoreResult nightOverseas = checkNightHighAmountOverseas(transactionId, amount, country);
            if (nightOverseas.blocked()) return nightOverseas;

            // 규칙 3: 블랙리스트 기기/IP
            FraudScoreResult blacklist = checkBlacklist(transactionId, deviceId, ipAddress);
            if (blacklist.blocked()) return blacklist;

            return FraudScoreResult.pass(transactionId);

        } catch (Exception e) {
            log.warn("RuleEvaluator failed, passing through | txId={} | error={}", transactionId, e.getMessage());
            return FraudScoreResult.pass(transactionId); // Fallback
        } finally {
            log.info("RuleEvaluator completed | txId={} | elapsed={}ms",
                    transactionId, System.currentTimeMillis() - start);
        }
    }

    // 규칙 1: 60초 내 5건 이상 → 차단
    private FraudScoreResult checkHighFrequency(String transactionId, String userId) {
        try {
            String key = "user:" + userId + ":transactions";
            double now = Instant.now().toEpochMilli();
            double windowStart = now - WINDOW_MS;
            Long count = redisTemplate.opsForZSet().count(key, windowStart, now);
            if (count != null && count >= HIGH_FREQUENCY_THRESHOLD) {
                return FraudScoreResult.block(transactionId, "HIGH_FREQUENCY",
                        "60초 내 " + count + "건 거래 탐지");
            }
        } catch (Exception e) {
            log.warn("checkHighFrequency failed | txId={} | error={}", transactionId, e.getMessage());
        }
        return FraudScoreResult.pass(transactionId);
    }

    // 규칙 2: 심야(22:00~06:00) + 100만원 이상 + 해외 결제 → 차단
    private FraudScoreResult checkNightHighAmountOverseas(String transactionId,
                                                          BigDecimal amount, String country) {
        LocalTime now = LocalTime.now();
        boolean isNight = now.isAfter(LocalTime.of(22, 0)) || now.isBefore(LocalTime.of(6, 0));
        boolean isHighAmount = amount.compareTo(HIGH_AMOUNT_THRESHOLD) >= 0;
        boolean isOverseas = !"KR".equals(country);

        if (isNight && isHighAmount && isOverseas) {
            return FraudScoreResult.block(transactionId, "NIGHT_HIGH_AMOUNT_OVERSEAS",
                    "심야 고액 해외 결제 탐지 | amount=" + amount + " | country=" + country);
        }
        return FraudScoreResult.pass(transactionId);
    }

    // 규칙 3: 블랙리스트 기기/IP → 차단
    private FraudScoreResult checkBlacklist(String transactionId, String deviceId, String ipAddress) {
        try {
            Boolean deviceBlacklisted = redisTemplate.opsForSet().isMember("blacklist:devices", deviceId);
            if (Boolean.TRUE.equals(deviceBlacklisted)) {
                return FraudScoreResult.block(transactionId, "BLACKLIST_DEVICE",
                        "블랙리스트 기기 탐지 | deviceId=" + deviceId);
            }
            Boolean ipBlacklisted = redisTemplate.opsForSet().isMember("blacklist:ips", ipAddress);
            if (Boolean.TRUE.equals(ipBlacklisted)) {
                return FraudScoreResult.block(transactionId, "BLACKLIST_IP",
                        "블랙리스트 IP 탐지 | ipAddress=" + ipAddress);
            }
        } catch (Exception e) {
            log.warn("checkBlacklist failed | txId={} | error={}", transactionId, e.getMessage());
        }
        return FraudScoreResult.pass(transactionId);
    }
}