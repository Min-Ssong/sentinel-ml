package com.sentinel.api.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisServiceTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private RedisService redisService;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        redisService = new RedisService(redisTemplate);
    }

    @Test
    @DisplayName("거래 이력이 Sorted Set에 정상 적재된다")
    void saveTransactionHistory_shouldAddToSortedSet() {
        redisService.saveTransactionHistory("user-001", "txId-001");

        verify(zSetOperations).add(eq("user:user-001:transactions"), eq("txId-001"), anyDouble());
        verify(redisTemplate).expire(eq("user:user-001:transactions"), eq(10L), eq(TimeUnit.MINUTES));
    }

    @Test
    @DisplayName("60초 내 거래 건수를 정상 조회한다")
    void countRecentTransactions_shouldReturnCount() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble())).thenReturn(3L);

        long count = redisService.countRecentTransactions("user-001");

        assertThat(count).isEqualTo(3L);
    }

    @Test
    @DisplayName("Redis 장애 시 거래 건수 조회는 0을 반환한다")
    void countRecentTransactions_shouldFallbackToZero() {
        when(zSetOperations.count(anyString(), anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Redis connection failed"));

        long count = redisService.countRecentTransactions("user-001");

        assertThat(count).isZero();
    }

    @Test
    @DisplayName("카운터 증가 시 TTL이 설정된다")
    void incrementCounter_shouldSetTtl() {
        redisService.incrementCounter("ip:1.1.1.1:count");

        verify(valueOperations).increment("ip:1.1.1.1:count");
        verify(redisTemplate).expire(eq("ip:1.1.1.1:count"), eq(60L), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Redis 장애 시 카운터 조회는 0을 반환한다")
    void getCounter_shouldFallbackToZero() {
        when(valueOperations.get(anyString()))
                .thenThrow(new RuntimeException("Redis connection failed"));

        long count = redisService.getCounter("ip:1.1.1.1:count");

        assertThat(count).isZero();
    }
}