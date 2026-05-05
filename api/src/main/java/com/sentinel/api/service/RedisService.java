package com.sentinel.api.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Service
public class RedisService {

    private static final Logger log = LoggerFactory.getLogger(RedisService.class);
    private static final long WINDOW_SECONDS = 60L;

    private final RedisTemplate<String, String> redisTemplate;

    public RedisService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // 사용자별 거래 이력 저장 (Sorted Set)
    public void saveTransactionHistory(String userId, String transactionId) {
        try {
            String key = "user:" + userId + ":transactions";
            double score = Instant.now().toEpochMilli();
            redisTemplate.opsForZSet().add(key, transactionId, score);
            redisTemplate.expire(key, 10, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.warn("Redis saveTransactionHistory failed | userId={} | error={}", userId, e.getMessage());
        }
    }

    // 60초 내 거래 건수 조회 (고빈도 탐지용)
    public long countRecentTransactions(String userId) {
        try {
            String key = "user:" + userId + ":transactions";
            double now = Instant.now().toEpochMilli();
            double windowStart = now - (WINDOW_SECONDS * 1000);
            Long count = redisTemplate.opsForZSet().count(key, windowStart, now);
            return count != null ? count : 0L;
        } catch (Exception e) {
            log.warn("Redis countRecentTransactions failed | userId={} | error={}", userId, e.getMessage());
            return 0L; // Fallback: 조회 실패 시 0 반환 → Rule Engine pass 처리
        }
    }

    // IP/기기별 Sliding Window Counter 증가
    public void incrementCounter(String key) {
        try {
            redisTemplate.opsForValue().increment(key);
            redisTemplate.expire(key, WINDOW_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Redis incrementCounter failed | key={} | error={}", key, e.getMessage());
        }
    }

    // IP/기기별 카운터 조회
    public long getCounter(String key) {
        try {
            String value = redisTemplate.opsForValue().get(key);
            return value != null ? Long.parseLong(value) : 0L;
        } catch (Exception e) {
            log.warn("Redis getCounter failed | key={} | error={}", key, e.getMessage());
            return 0L; // Fallback
        }
    }
}