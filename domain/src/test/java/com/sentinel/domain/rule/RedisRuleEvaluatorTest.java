package com.sentinel.domain.rule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisRuleEvaluatorTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private SetOperations<String, String> setOperations;

    private RedisRuleEvaluator evaluator;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        evaluator = new RedisRuleEvaluator(redisTemplate);
    }

    // 규칙 1: 고빈도 — 차단 케이스
    @Test
    @DisplayName("60초 내 5건 이상 거래 시 차단")
    void highFrequency_shouldBlock() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble())).thenReturn(5L);

        FraudScoreResult result = evaluator.evaluate(
                "txId-001", "user-001", new BigDecimal("10000"), "KR", "device-1", "1.1.1.1");

        assertThat(result.blocked()).isTrue();
        assertThat(result.triggeredRule()).isEqualTo("HIGH_FREQUENCY");
    }

    // 규칙 1: 고빈도 — 통과 케이스
    @Test
    @DisplayName("60초 내 4건 이하 거래 시 통과")
    void highFrequency_shouldPass() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble())).thenReturn(4L);

        FraudScoreResult result = evaluator.evaluate(
                "txId-002", "user-002", new BigDecimal("10000"), "KR", "device-2", "1.1.1.2");

        assertThat(result.blocked()).isFalse();
    }

    // 규칙 3: 블랙리스트 기기 — 차단 케이스
    @Test
    @DisplayName("블랙리스트 기기 탐지 시 차단")
    void blacklistDevice_shouldBlock() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble())).thenReturn(1L);
        when(setOperations.isMember("blacklist:devices", "device-evil")).thenReturn(true);

        FraudScoreResult result = evaluator.evaluate(
                "txId-003", "user-003", new BigDecimal("10000"), "KR", "device-evil", "1.1.1.3");

        assertThat(result.blocked()).isTrue();
        assertThat(result.triggeredRule()).isEqualTo("BLACKLIST_DEVICE");
    }

    // 규칙 3: 블랙리스트 IP — 차단 케이스
    @Test
    @DisplayName("블랙리스트 IP 탐지 시 차단")
    void blacklistIp_shouldBlock() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble())).thenReturn(1L);
        when(setOperations.isMember("blacklist:devices", "device-1")).thenReturn(false);
        when(setOperations.isMember("blacklist:ips", "9.9.9.9")).thenReturn(true);

        FraudScoreResult result = evaluator.evaluate(
                "txId-004", "user-004", new BigDecimal("10000"), "KR", "device-1", "9.9.9.9");

        assertThat(result.blocked()).isTrue();
        assertThat(result.triggeredRule()).isEqualTo("BLACKLIST_IP");
    }

    // Redis Fallback — Redis 장애 시 통과
    @Test
    @DisplayName("Redis 장애 시 Fallback으로 통과 처리")
    void redisFallback_shouldPass() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Redis connection failed"));

        FraudScoreResult result = evaluator.evaluate(
                "txId-005", "user-005", new BigDecimal("10000"), "KR", "device-5", "1.1.1.5");

        assertThat(result.blocked()).isFalse();
    }
}